"""
Transmission Module

Multi-threaded processing of transmission data
"""

import sys
import socket
import random
import threading
import time
import traceback

from plc import PlcModule
from urp import *
from logger import Logger
from timer import Timer

# ========================================
# Constant definition
# ========================================
# State constants
STATE_CLOSED = 0
STATE_SYN_SENT = 1
STATE_ESTABLISHED = 2
STATE_CLOSING = 3
STATE_FIN_WAIT = 4

# Other constants
MAX_SEQ_NUM = 65536  # Max sequence number: 2^16
MSS = 1000  # Max length of data


class Sender:
    """Sender Class


    Multithreaded Processing:
        Thread 1: Transmit Thread - Window space available and new data remains to be sent
        Thread 2: Receive Thread - Handles ACK reception events
        Thread 3: Timer Thread - Handles timeout retransmissions
        Main Thread: Coordinates all threads and manages state transitions

    Attributes:
        sender_port (int): Sender port
        receiver_port (int): Receiver port
        filename (str): File to send
        max_win (int): Maximum transmission window
        rto (float): Timeout (in ms)
        flp (float): Forward loss rate
        rlp (float): Round-trip loss percentage
        fcp (float): Forward error correction percentage
        rcp (float): Round-trip correction percentage

        plc (PlcModule): Plc module
        sock (socket): UDP socket
        state (int): Current sender state
        isn (int): Initial sequence number
        next_seq_num (int): Next segment sequence number
        send_buffer (dict): Send buffer {seq_num: (segment, send_time)}

        timer_thread (Thread): Timer thread
        timer_running (bool): Timer running flag
        timer_lock

        dup_ack_count (int): Duplicate ACK counter
        last_ack_num (int): Last received ACK
        log_file (file): Log file
        start_time (time): Sender start time
        stats (dict): Statistics

        file_handle (file): File to read
        file_size (int): File size
        file_pos (int): Current byte position within the file
        all_data_sent (bool): Send completion flag

        running (bool): Thread running flag
    """

    def __init__(
        self, sender_port, receiver_port, filename, max_win, rto, flp, rlp, fcp, rcp
    ) -> None:
        self.sender_port = sender_port
        self.receiver_port = receiver_port
        self.filename = filename
        self.max_win = max_win
        self.rto = rto / 1000.0  # Convert to seconds

        # Initialize PLC Module
        self.plc = PlcModule(flp, rlp, fcp, rcp)

        # Create a UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", sender_port))
        self.sock.settimeout(0.01)

        # State
        self.state_lock = threading.Lock()
        self.state = STATE_CLOSED

        # Sequence number
        self.isn = random.randint(0, MAX_SEQ_NUM - 1)
        self.next_seq_num = (self.isn + 1) % MAX_SEQ_NUM
        self.base = self.isn

        # Send Buffer (Unacknowledged Segments)
        self.buffer_lock = threading.RLock()
        self.send_buffer = {}

        # Timer
        self.timer = Timer(
            timeout=self.rto, callback=self.handle_timeout, auto_restart=True
        )

        # Duplicate ACK Count
        self.ack_lock = threading.RLock()
        self.dup_ack_count = 0
        self.last_ack_num = 0

        # Log file
        self.log_lock = threading.RLock()
        self.log = Logger("sender_log.txt")

        # Statistics
        self.stats_lock = threading.RLock()
        self.stats = {
            "original_data_sent": 0,
            "total_data_sent": 0,
            "original_segments_sent": 0,
            "total_segments_sent": 0,
            "timeout_retrans": 0,
            "fast_retrans": 0,
            "dup_acks_received": 0,
            "corrupted_acks_discarded": 0,
        }

        # Files to be sent
        self.file_lock = threading.RLock()
        self.file_handle = None
        self.file_size = 0
        self.file_pos = 0
        self.all_data_sent = False

        # Thread control
        self.running = True
        self.send_thread = None
        self.recv_thread = None

        # Event Notification
        # Notify when window has space
        self.window_available_event = threading.Event()
        self.window_available_event.set()

    def run(self):
        """Run the sender's main process"""
        try:
            # Open file
            self.file_handle = open(self.filename, "rb")

            # Obtain the file size
            self.file_handle.seek(0, 2)
            self.file_size = self.file_handle.tell()
            self.file_handle.seek(0, 0)

            print(f"Total size of file: {self.file_size} Bytes")

            # Begin establishing the connection
            self.connection_setup()

            # Data Transmission
            self.data_transfer()

            # Connection Closed
            self.connection_close()

            # Write to the log file
            self.write_stats()

        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()

        finally:
            self.cleanup()

    def connection_setup(self):
        """Connection Establishment Phase (SYN, ACK)"""
        print("Begin establishing the connection...")

        with self.state_lock:
            self.state = STATE_SYN_SENT

        # Send SYN
        self.start_time = time.time()
        syn_segment = UrpSegment(self.isn, FLAG_SYN, b"")
        self.send_segment(syn_segment, is_new=True)

        # Waiting for ACK (polling method, simpler connection establishment)
        while True:
            with self.state_lock:
                if self.state == STATE_ESTABLISHED:
                    break

            # Receive ACK
            if self.check_for_ack(timeout=0.01):
                break

            # Check if timeout
            self.checktimeout()

    def data_transfer(self):
        """Data transmission phase"""
        with self.state_lock:
            self.state = STATE_ESTABLISHED

        # Start the ACK reception thread
        self.recv_thread = threading.Thread(target=self.receive_ack_thread, daemon=True)
        self.recv_thread.start()

        # Start the sending thread
        self.send_thread = threading.Thread(target=self.send_data_thread, daemon=True)
        self.send_thread.start()

        # The main thread waits for all data to be sent.
        assert self.file_handle is not None
        while True:
            # Check if data transmission is complete: both file_data and send_buffer are empty
            with self.file_lock:
                file_done = self.file_pos >= self.file_size

            with self.buffer_lock:
                buffer_empty = len(self.send_buffer) == 0

            if file_done and buffer_empty:
                break

            time.sleep(0.01)

        # Stop thread
        self.running = False
        self.timer.stop()

        # Wait for the thread to finish
        if self.recv_thread:
            self.recv_thread.join(timeout=1.0)
        if self.send_thread:
            self.send_thread.join(timeout=1.0)

        with self.state_lock:
            self.state = STATE_CLOSING

    def connection_close(self):
        """Connection Closed (FIN, ACK)"""

        # CLOSING state: Ensure all data has been received before closing.
        while True:
            with self.buffer_lock:
                if not self.send_buffer:
                    break
                time.sleep(0.01)

        # Send FIN
        with self.state_lock:
            self.state = STATE_FIN_WAIT

        with self.buffer_lock:
            fin_seq = self.next_seq_num

        fin_segment = UrpSegment(fin_seq, FLAG_FIN, b"")
        self.send_segment(fin_segment, is_new=True)

        # Wait for ACK
        while True:
            with self.state_lock:
                if self.state == STATE_CLOSED:
                    break

            # Receive ACK
            if self.check_for_ack(timeout=0.01):
                break

            # Check if timeout
            self.checktimeout()

    # ========================================
    # Event 1: Window space is available and new data remains to be sent
    # ========================================
    def send_data_thread(self):
        """Data Transmission Thread"""
        while self.running:
            # Wait for Window Available Event
            self.window_available_event.wait(timeout=0.01)

            with self.state_lock:
                if self.state != STATE_ESTABLISHED:
                    break

            # Send data
            self.try_send_data()

    def try_send_data(self):
        """Try to send data"""
        assert self.file_handle is not None
        while True:
            # Check if the window has available space
            with self.buffer_lock:
                window_used = self._calculate_window_usage()
                if window_used >= self.max_win:
                    self.window_available_event.clear()
                    return

            # Check if there is any more data to send
            with self.file_lock:
                if self.file_pos >= self.file_size:
                    self.all_data_sent = True
                    return

                # Read data
                read_length = min(MSS, self.file_size - self.file_pos)
                data = self.file_handle.read(read_length)
                self.file_pos += len(data)

                # Obtain sequence number
                with self.buffer_lock:
                    seq_num = self.next_seq_num
                    self.next_seq_num = (seq_num + len(data)) % MAX_SEQ_NUM

            # Create and send segment
            segment = UrpSegment(seq_num, FLAG_DATA, data)
            self.send_segment(segment, is_new=True)

            # Update statistics
            with self.stats_lock:
                self.stats["original_data_sent"] += len(data)

            # If this is the first unconfirmed segment, start the timer
            with self.buffer_lock:
                if len(self.send_buffer) == 1:
                    self.timer.start()

    def _calculate_window_usage(self):
        """Calculate the current window usage"""
        total_bytes = 0
        for (segment, _) in self.send_buffer.values():
            if segment and segment.flags == FLAG_DATA:
                total_bytes += len(segment.data)

        return total_bytes

    # ========================================
    # Event 2: Timeout event
    # ========================================
    def handle_timeout(self):
        """Handle timeout by retransmitting the oldest segment"""
        with self.buffer_lock:
            if not self.send_buffer:
                return

            self.retrans_oldest()

    def checktimeout(self):
        """Timeout Check

        Applies only to connection establishment and termination
        """
        with self.buffer_lock:
            if not self.send_buffer:
                return

            # Check if timeout
            if time.time() - self.send_buffer[self.base][1] >= self.rto:
                print(
                    "A timeout was detected during the connect or close phase; SYN/FIN retransmission has begun."
                )
                self.retrans_oldest()

    def retrans_oldest(self, retrans_type="timeout_retrans"):
        """Resend the oldest segment

        Args:
            retrans_type (str): timeout or fast_retrans
        """
        print("Begin retransmission...")
        with self.buffer_lock:
            if not self.send_buffer:
                return

            print(f"Current window sequence number starting point: {self.base}")
            segment, _ = self.send_buffer[self.base]

            if segment:
                print(f"Retransmission Sequence Number: {segment.seq_num}")
                self.send_segment(segment)

                with self.stats_lock:
                    self.stats[retrans_type] += 1

                # Update send time
                self.send_buffer[self.base] = (segment, time.time())

    # ========================================
    # Event 3: Receive ACK
    # ========================================
    def receive_ack_thread(self):
        """Receive ACK Thread"""
        self.sock.settimeout(0.01)

        while self.running:
            self.receive_ack()

    def check_for_ack(self, timeout):
        """Check for ACK arrival

        Only for connection establishment and termination
        """
        self.sock.settimeout(timeout)

        return self.receive_ack()

    def receive_ack(self):
        """Receive ACK Segment"""
        try:
            data, addr = self.sock.recvfrom(2048)

            # PLC process data
            processed_data, status = self.plc.process_bk(data)

            # Analyzing the ACK Segment
            ack_segment = UrpSegment.unpack(data)
            if not ack_segment:
                return

            print(f"Receive ACK: {ack_segment.seq_num}")

            if status == "cor":
                print(f"ACK corrupted: {ack_segment.seq_num}")
                with self.stats_lock:
                    self.stats["corrupted_acks_discarded"] += 1

            elif status == "ok":
                # Verify checksum
                if not ack_segment.verify_checksum():
                    status = "cor"
                    with self.stats_lock:
                        self.stats["corrupted_acks_discarded"] += 1
                else:
                    print("ACK verification complete, processing begins...")
                    self.process_ack(ack_segment)

            else:
                print(f"ACK dropped: {ack_segment.seq_num}")

            with self.log_lock:
                self.log.log_segment("rcv", status, ack_segment, 0)

            return status == "ok"

        except socket.timeout:
            pass

        except Exception as e:
            print(f"Error: {e}")

    def process_ack(self, segment: UrpSegment):
        """Process ACK segment"""
        ack_num = segment.seq_num

        with self.state_lock:
            current_state = self.state

        # Handle ACK Based on Status
        if current_state == STATE_SYN_SENT:
            # Expected ACK = ISN + 1
            expected_ack = (self.isn + 1) % MAX_SEQ_NUM

            if ack_num == expected_ack:
                with self.state_lock:
                    self.state = STATE_ESTABLISHED
                with self.buffer_lock:
                    self.send_buffer.clear()  # Clear buffer

        elif self.state == STATE_ESTABLISHED or self.state == STATE_CLOSING:
            # Check if it is a new ACK
            if self._is_new_ack(ack_num):
                with self.buffer_lock:
                    # Slide the window to the current acknowledgment,
                    # which is also the starting position of the next segment to be sent
                    self.base = ack_num

                    # Remove confirmed paragraphs
                    segments_remove = []
                    for seq in self.send_buffer.keys():
                        if self._is_before(seq, ack_num):
                            segments_remove.append(seq)

                    for seq in segments_remove:
                        print(f"Delete sequence numbers in the send buffer: {seq}")

                        del self.send_buffer[seq]

                        self._print_buffer()

                    has_unacked = len(self.send_buffer) > 0

                # Reset the counter
                with self.ack_lock:
                    self.dup_ack_count = 0
                    self.last_ack_num = ack_num

                # Reset timer
                if has_unacked:
                    print("Received new ACK, restart timer")
                    self.timer.restart()
                else:
                    # All segments have been confirmed, stop the timer
                    print("All segments have been confirmed, stop the timer")
                    self.timer.stop()

                # Free up available space
                self.window_available_event.set()

            else:
                # It is a duplicate ACK
                with self.ack_lock:
                    if ack_num == self.last_ack_num:
                        self.dup_ack_count += 1

                        with self.stats_lock:
                            self.stats["dup_acks_received"] += 1

                        # Fast retransmission
                        if self.dup_ack_count == 3:
                            print("Fast retransmission")
                            self.retrans_oldest("fast_retrans")
                            self.dup_ack_count = 0

        elif self.state == STATE_FIN_WAIT:
            with self.buffer_lock:
                expected_ack = (self.next_seq_num + 1) % MAX_SEQ_NUM

            if ack_num == expected_ack:
                with self.state_lock:
                    self.state = STATE_CLOSED
                with self.buffer_lock:
                    self.send_buffer.clear()
                self.timer.stop()

    def send_segment(self, segment: UrpSegment, is_new=False):
        """Transmit Section (via PLC Module)"""
        segment_bytes = segment.pack()

        # Processed via PLC
        processed_data, status = self.plc.process_fd(segment_bytes)

        # Logging
        with self.log_lock:
            self.log.log_segment("snd", status, segment, len(segment.data))

        # If not discarded, successfully sent
        if status != "drp":
            assert processed_data is not None
            self.sock.sendto(processed_data, ("127.0.0.1", self.receiver_port))

        # If it's a new data segment or a retransmission, add it to the buffer.
        if segment.flags != FLAG_ACK:
            with self.buffer_lock:
                self.send_buffer[segment.seq_num] = (segment, time.time())

        # Update statistics
        with self.stats_lock:
            if is_new:
                self.stats["original_segments_sent"] += 1

            self.stats["total_segments_sent"] += 1

            if segment.flags == FLAG_DATA:
                self.stats["total_data_sent"] += len(segment.data)

    def write_stats(self):
        """Write statistics"""
        with self.log_lock:
            self.log.log_file.write("\n")
            self.log.log_file.write(
                f"Original data sent:             {self.stats['original_data_sent']}\n"
            )
            self.log.log_file.write(
                f"Total data sent:                {self.stats['total_data_sent']}\n"
            )
            self.log.log_file.write(
                f"Original segments sent:         {self.stats['original_segments_sent']}\n"
            )
            self.log.log_file.write(
                f"Total segments sent:            {self.stats['total_segments_sent']}\n"
            )
            self.log.log_file.write(
                f"Timeout retransmissions:        {self.stats['timeout_retrans']}\n"
            )
            self.log.log_file.write(
                f"Fast retransmissions:           {self.stats['fast_retrans']}\n"
            )
            self.log.log_file.write(
                f"Duplicate acks received:        {self.stats['dup_acks_received']}\n"
            )
            self.log.log_file.write(
                f"Corrupted acks discarded:       {self.stats['corrupted_acks_discarded']}\n"
            )
            self.log.log_file.write(
                f"PLC forward segments dropped:   {self.plc.fwd_dropped}\n"
            )
            self.log.log_file.write(
                f"PLC forward segments corrupted: {self.plc.fwd_corrupted}\n"
            )
            self.log.log_file.write(
                f"PLC reverse segments dropped:   {self.plc.rev_dropped}\n"
            )
            self.log.log_file.write(
                f"PLC reverse segments corrupted: {self.plc.rev_corrupted}\n"
            )

    def cleanup(self):
        self.running = False
        self.timer.stop()

        if self.file_handle:
            self.file_handle.close()

        if self.log.log_file:
            self.log.log_file.close()

        if self.sock:
            self.sock.close()

        sys.exit(1)

    def _is_new_ack(self, ack_num):
        """Determine whether it is a new ACK"""
        with self.buffer_lock:
            return self._is_before(self.base, ack_num)

    def _is_before(self, seq1, seq2):
        """Determine whether seq1 is older than seq2

        Returns:
            bool: True - seq1 is older than seq2
        """
        diff = (seq2 - seq1) % MAX_SEQ_NUM
        return 0 < diff <= MAX_SEQ_NUM // 2

    def _print_buffer(self):
        print("Current buffer: ", end="")
        for seq_num in self.send_buffer.keys():
            print(f"{seq_num}, ", end="")
        print()


# ========================================
# Main function
# ========================================
def main():
    if len(sys.argv) != 10:
        print(
            "Usage: python3 sender.py sender_port receiver_port txt_file_to_send max_win rto flp rlp fcp rcp"
        )
        sys.exit(1)

    sender_port = int(sys.argv[1])
    receiver_port = int(sys.argv[2])
    file_name = sys.argv[3]
    max_win = int(sys.argv[4])
    rto = int(sys.argv[5])
    flp = float(sys.argv[6])
    rlp = float(sys.argv[7])
    fcp = float(sys.argv[8])
    rcp = float(sys.argv[9])

    if not 49152 <= sender_port <= 65535:
        print("Args Error: sender_port should be in [49152, 65535].")
        sys.exit(1)

    if not max_win < MAX_SEQ_NUM / 2:
        print(f"Args Error: max_win > {MAX_SEQ_NUM}.")
        sys.exit(1)
    elif max_win % 1000 != 0:
        print(f"Args Error: max_win should be a multiple of 1000.")
        sys.exit(1)

    sender = Sender(
        sender_port, receiver_port, file_name, max_win, rto, flp, rlp, fcp, rcp
    )
    sender.run()


if __name__ == "__main__":
    main()
