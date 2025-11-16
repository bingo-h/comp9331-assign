"""
Receive Module

Single-threaded processing of received data
"""

import sys
import socket
import time

from urp import *
from logger import Logger
from timer import Timer

# ========================================
# Constant definition
# ========================================
# State constants
STATE_CLOSED = 0
STATE_LISTEN = 1
STATE_ESTABLISHED = 2
STATE_TIME_WAIT = 3

# Other constants
MAX_SEQ_NUM = 65536  # Max sequence number: 2^16
MSS = 1000  # Max length of data
MSL = 1  # 1s
TIME_WAIT_DURATION = 5 * MSL  # Wait for 5s


class Receiver:
    """URP Receiver

    Attributes:
        receiver_port (int): Receiving port
        recv_buffer (dict): Receive buffer {seq_num: segment_data}
    """

    def __init__(self, receiver_port, sender_port, file_name, max_win) -> None:
        self.server_ip = "127.0.0.1"
        self.receiver_port = receiver_port
        self.sender_port = sender_port
        self.file_name = file_name
        self.max_win = max_win

        # Create socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.server_ip, receiver_port))
        self.sock.settimeout(0.1)

        # Initialize state
        self.state = STATE_CLOSED
        self.expected_seq_num = 0

        # Receive Buffer
        self.recv_buffer = {}

        # Output File
        self.output_file = None

        # Log
        self.start_time = None
        self.log = Logger("receiver_log.txt")

        # Statistics
        self.stats = {
            "original_data_received": 0,
            "total_data_received": 0,
            "original_segments_received": 0,
            "total_segments_received": 0,
            "corrupted_segments_discarded": 0,
            "duplicate_segments_received": 0,
            "total_acks_sent": 0,
            "duplicate_acks_sent": 0,
        }

        # Set of received segment sequence numbers for duplicate detection
        self.received_seq_num = set()

        # TIME_WAIT timer
        self.timer = Timer(timeout=TIME_WAIT_DURATION, callback=self.wait_expired)

        # Running flag
        self.running = True

    def run(self):
        """Receiving End Main Thread"""
        try:
            # Open output file
            self.output_file = open(self.file_name, "wb")

            # Enter LISTEN state
            print("Begin listening")
            self.state = STATE_LISTEN

            # Main loop
            while self.running and self.state != STATE_CLOSED:
                self.receive_segment()
                time.sleep(0.001)

            # Write Statistics
            self.write_stats()

        except Exception as e:
            print(f"Error (Receiver): {e}")

        finally:
            self.cleanup()

    def receive_segment(self):
        """Receive and process segment"""
        try:
            data, addr = self.sock.recvfrom(2048)

            # Record start_time
            if self.start_time is None:
                self.start_time = time.time()

            # Analysis Section
            segment = UrpSegment.unpack(data)
            if not segment:
                return

            print("Received a segment...")

            self.stats["total_segments_received"] += 1

            # Verify checksum
            if not segment.verify_checksum():
                self.log.log_segment("rcv", "cor", segment, len(segment.data))
                self.stats["corrupted_segments_discarded"] += 1
                print(
                    f"Corrupted segment: {self.stats['corrupted_segments_discarded']}"
                )
                return

            # Logging (Normal Reception)
            self.log.log_segment("rcv", "ok", segment, len(segment.data))

            # Process received data based on status
            if self.state == STATE_LISTEN:
                print(f"STATE_LISTEN: {segment.seq_num}")
                self.handle_listen(segment)
            elif self.state == STATE_ESTABLISHED:
                print(f"STATE_ESTABLISHED: {segment.seq_num}")
                self.handle_established(segment)
            elif self.state == STATE_TIME_WAIT:
                print(f"STATE_TIME_WAIT: {segment.seq_num}")
                self.handle_wait(segment)

        except socket.timeout:
            pass

        except Exception as e:
            pass

    def handle_listen(self, segment: UrpSegment):
        """Processing data received in the LISTEN state"""

        if segment.flags & FLAG_SYN:
            # Received SYN signal, send ACK
            print("Initiating connection, sending ACK...")
            self.expected_seq_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(self.expected_seq_num)
            self.state = STATE_ESTABLISHED
            self.stats["original_segments_received"] += 1

    def handle_established(self, segment: UrpSegment):
        """Process data received in the ESTABLISHED state"""
        if segment.flags & FLAG_SYN:
            # Received duplicate SYN, retransmit ACK
            print("Received duplicate SYN, retransmit ACK")
            self.expected_seq_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(self.expected_seq_num)
            self.stats["duplicate_segments_received"] += 1

        elif segment.flags & FLAG_FIN:
            # Upon receiving a FIN, enter the TIME_WAIT state
            print("Upon receiving a FIN, enter the TIME_WAIT state")
            ack_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(ack_num)
            self.state = STATE_TIME_WAIT

            # Start TIME_WAIT timer
            self.timer.start()

        elif segment.flags == FLAG_DATA:
            # Received DATA segment
            print("Received DATA segment")
            self.process_data_segment(segment)

    def handle_wait(self, segment: UrpSegment):
        """Handling data received while in the TIME_WAIT state"""
        if segment.flags & FLAG_FIN:
            # Repeated FIN, retransmit ACK
            print("Repeated FIN, retransmit ACK")
            ack_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(ack_num)
            self.stats["duplicate_segments_received"] += 1

    def process_data_segment(self, segment: UrpSegment):
        """Process received DATA segment"""

        self.stats["total_data_received"] += len(segment.data)

        # Check if repeated
        is_duplicate = segment.seq_num in self.received_seq_num

        # Update statistics
        if not is_duplicate:
            self.stats["original_segments_received"] += 1
            self.stats["original_data_received"] += len(segment.data)
            self.received_seq_num.add(segment.seq_num)
        else:
            self.stats["duplicate_segments_received"] += 1
            self.send_ack(self.expected_seq_num)
            return

        if segment.seq_num == self.expected_seq_num:
            # If the received data matches the current expectation,
            # write it directly to the file.
            assert self.output_file is not None
            self.output_file.write(segment.data)
            self.expected_seq_num = (
                self.expected_seq_num + len(segment.data)
            ) % MAX_SEQ_NUM

            # Check buffer
            self.flush_buffer()
        else:
            # Out-of-order, store in buffer
            self.recv_buffer[segment.seq_num] = segment.data

        # Send ACK
        self.send_ack(self.expected_seq_num)

    def flush_buffer(self):
        """Write contiguous data from the buffer"""
        while self.expected_seq_num in self.recv_buffer:
            data = self.recv_buffer[self.expected_seq_num]

            assert self.output_file is not None
            self.output_file.write(data)

            del self.recv_buffer[self.expected_seq_num]

            self.expected_seq_num = (self.expected_seq_num + len(data)) % MAX_SEQ_NUM

    def send_ack(self, ack_num):
        """Send ACK segment"""
        ack_segment = UrpSegment(ack_num, FLAG_ACK, b"")
        segment_data = ack_segment.pack()

        print(f"Send ACK: {ack_num}")
        self.sock.sendto(segment_data, (self.server_ip, self.sender_port))

        # Record segment log
        self.log.log_segment("snd", "ok", ack_segment, 0)

        # Update statistics
        self.stats["total_acks_sent"] += 1

        # Check whether duplicate ACKs were sent
        if hasattr(self, "last_ack_sent"):
            if self.last_ack_sent == ack_num:
                self.stats["duplicate_acks_sent"] += 1

        self.last_ack_sent = ack_num

    def wait_expired(self):
        print("Receiving FIN, shutting down... (Wait for 5 seconds)")
        time.sleep(TIME_WAIT_DURATION)
        self.state = STATE_CLOSED
        self.running = False

    def write_stats(self):
        """Write statistics"""

        self.log.log_file.write("\n")
        self.log.log_file.write(
            f"Original data received:        {self.stats['original_data_received']}\n"
        )
        self.log.log_file.write(
            f"Total data received:           {self.stats['total_data_received']}\n"
        )
        self.log.log_file.write(
            f"Original segments received:    {self.stats['original_segments_received']}\n"
        )
        self.log.log_file.write(
            f"Total segments received:       {self.stats['total_segments_received']}\n"
        )
        self.log.log_file.write(
            f"Corrupted segments discarded:  {self.stats['corrupted_segments_discarded']}\n"
        )
        self.log.log_file.write(
            f"Duplicate segments received:   {self.stats['duplicate_segments_received']}\n"
        )
        self.log.log_file.write(
            f"Total acks sent:               {self.stats['total_acks_sent']}\n"
        )
        self.log.log_file.write(
            f"Duplicate acks sent:           {self.stats['duplicate_acks_sent']}\n"
        )

    def cleanup(self):
        """Clean up resources"""
        if self.output_file:
            self.output_file.close()
        if self.log.log_file:
            self.log.log_file.close()
        if self.sock:
            self.sock.close()

        sys.exit(1)


# ========================================
# 主函数
# ========================================
def main():
    if len(sys.argv) != 5:
        print(
            "Usage: python3 receiver.py receiver_port sender_port txt_file_received max_win"
        )
        sys.exit(1)

    receiver_port = int(sys.argv[1])
    sender_port = int(sys.argv[2])
    filename = sys.argv[3]
    max_win = int(sys.argv[4])

    if not 49152 <= sender_port <= 65535:
        print("Args Error: sender_port should be in [49152, 65535].")
        sys.exit(1)

    if not max_win < MAX_SEQ_NUM / 2:
        print(f"Args Error: max_win > {MAX_SEQ_NUM}.")
        sys.exit(1)
    elif max_win % 1000 != 0:
        print(f"Args Error: max_win should be a multiple of 1000.")
        sys.exit(1)

    receiver = Receiver(receiver_port, sender_port, filename, max_win)
    receiver.run()


if __name__ == "__main__":
    main()
