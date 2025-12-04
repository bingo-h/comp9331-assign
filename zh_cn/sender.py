"""
发送模块

多线程处理发送数据

Functions:
    TODO
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
# 常量定义
# ========================================
# 状态常量
STATE_CLOSED = 0
STATE_SYN_SENT = 1
STATE_ESTABLISHED = 2
STATE_CLOSING = 3
STATE_FIN_WAIT = 4

# 其他常量
MAX_SEQ_NUM = 65536  # 序列号最大值: 2^16
MSS = 1000  # 数据段最大值


class Sender:
    """发送端类

    多线程处理:
        线程1: 发送线程 - 窗口空间可用且仍有新数据未发
        线程2: 接收线程 - 处理ACK接收事件
        线程3: 定时器线程 - 处理超时重传
        主线程: 协调各线程，管理状态转换

    Attributes:
        sender_port (int): 发送端端口
        receiver_port (int): 接收端端口
        filename (str): 发送文件
        max_win (int): 最大发送窗口
        rto (float): 超时时间 (单位: ms)
        flp (float): 前向损失率
        rlp (float): 后向丢失率
        fcp (float): 前向损坏率
        rcp (float): 后向损坏率

        plc (PlcModule): Plc模块
        sock (socket): UDP Socket
        state (int): 发送端当前状态
        isn (int): 初始序列号
        next_seq_num (int): 下一段序列号
        send_buffer (dict): 发送缓冲区 {seq_num: (segment, send_time)}

        timer_thread ()
        timer_running (bool): 计时器运行标志
        timer_lock

        dup_ack_count (int): 重复ACK计数器
        last_ack_num (int): 上一次接收到的ACK
        log_file (file): 日志文件
        start_time (time): 发送端开始时间
        stats (dict): 统计信息

        file_handle (): 文件句柄
        file_size (int): 文件总大小
        file_pos (int): 当前在文件内的第几个字节
        all_data_sent (bool): 发送完成标志

        running (bool): 线程运行标志
    """

    def __init__(
        self, sender_port, receiver_port, filename, max_win, rto, flp, rlp, fcp, rcp
    ) -> None:
        self.sender_port = sender_port
        self.receiver_port = receiver_port
        self.filename = filename
        self.max_win = max_win
        self.rto = rto / 1000.0  # 转换为秒

        # 初始化PLC模块
        self.plc = PlcModule(flp, rlp, fcp, rcp)

        # 创建UDP套接字
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", sender_port))
        self.sock.settimeout(0.01)

        # 状态
        self.state_lock = threading.Lock()
        self.state = STATE_CLOSED

        # 序列号
        self.isn = random.randint(0, MAX_SEQ_NUM - 1)
        self.next_seq_num = (self.isn + 1) % MAX_SEQ_NUM
        self.base = self.isn

        # 发送缓冲区 (未确认段)
        self.buffer_lock = threading.RLock()
        self.send_buffer = {}

        # 定时器
        self.timer = Timer(
            timeout=self.rto, callback=self.handle_timeout, auto_restart=True
        )

        # 重复ACK计数
        self.ack_lock = threading.RLock()
        self.dup_ack_count = 0
        self.last_ack_num = 0

        # 日志文件
        self.log_lock = threading.RLock()
        self.log = Logger("sender_log.txt")

        # 统计信息
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

        # 要发送的文件
        self.file_lock = threading.RLock()
        self.file_handle = None
        self.file_size = 0
        self.file_pos = 0
        self.all_data_sent = False

        # 线程控制
        self.running = True
        self.send_thread = None
        self.recv_thread = None

        # 事件通知
        # 窗口有空间时通知
        self.window_available_event = threading.Event()
        self.window_available_event.set()

    def run(self):
        """运行发送端主流程"""
        try:
            # 打开文件
            self.file_handle = open(self.filename, "rb")

            # 获取文件大小
            self.file_handle.seek(0, 2)
            self.file_size = self.file_handle.tell()
            self.file_handle.seek(0, 0)

            print(f"文件总大小: {self.file_size} Bytes")

            # 开始建立连接
            self.connection_setup()

            # 数据传输
            self.data_transfer()

            # 连接关闭
            self.connection_close()

            # 写入日志文件
            self.write_stats()

        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()

        finally:
            self.cleanup()

    def connection_setup(self):
        """建立连接阶段 (SYN, ACK)"""
        print("开始建立连接...")

        with self.state_lock:
            self.state = STATE_SYN_SENT

        # 发送SYN
        self.start_time = time.time()
        syn_segment = UrpSegment(self.isn, FLAG_SYN, b"")
        self.send_segment(syn_segment, is_new=True)

        # 等待ACK (轮询方式，连接建立较简单)
        while True:
            with self.state_lock:
                if self.state == STATE_ESTABLISHED:
                    break

            # 接收ACK
            if self.check_for_ack(timeout=0.01):
                break

            # 检查超时
            self.checktimeout()

    def data_transfer(self):
        """数据传输阶段"""
        with self.state_lock:
            self.state = STATE_ESTABLISHED

        # 启动接收ACK线程
        self.recv_thread = threading.Thread(target=self.receive_ack_thread, daemon=True)
        self.recv_thread.start()

        # 启动发送线程
        self.send_thread = threading.Thread(target=self.send_data_thread, daemon=True)
        self.send_thread.start()

        # 主线程等待所有数据发送完毕
        while True:
            # 检查是否数据已经发送完: file_data 和 send_buffer 均空
            with self.file_lock:
                file_done = self.file_pos >= self.file_size

            with self.buffer_lock:
                buffer_empty = len(self.send_buffer) == 0

            if file_done and buffer_empty:
                break

            time.sleep(0.01)

        # 停止线程
        self.running = False
        self.timer.stop()

        # 等待线程结束
        if self.recv_thread:
            self.recv_thread.join(timeout=1.0)
        if self.send_thread:
            self.send_thread.join(timeout=1.0)

        with self.state_lock:
            self.state = STATE_CLOSING

    def connection_close(self):
        """连接关闭 (FIN, ACK)"""

        # CLOSING 状态: 确保所有数据都已被接收才能关闭
        while True:
            with self.buffer_lock:
                if not self.send_buffer:
                    break
                time.sleep(0.01)

        # 发送FIN
        with self.state_lock:
            self.state = STATE_FIN_WAIT

        with self.buffer_lock:
            fin_seq = self.next_seq_num

        fin_segment = UrpSegment(fin_seq, FLAG_FIN, b"")
        self.send_segment(fin_segment, is_new=True)

        # 等待ACK
        while True:
            with self.state_lock:
                if self.state == STATE_CLOSED:
                    break

            # 接收ACK
            if self.check_for_ack(timeout=0.01):
                break

            # 检查超时
            self.checktimeout()

    # ========================================
    # Event 1: 窗口空间可用且仍有新数据未发
    # ========================================
    def send_data_thread(self):
        """发送数据线程"""
        while self.running:
            # 等待窗口可用事件
            self.window_available_event.wait(timeout=0.01)

            with self.state_lock:
                if self.state != STATE_ESTABLISHED:
                    break

            # 发送数据
            self.try_send_data()

    def try_send_data(self):
        """尝试发送数据"""
        while True:
            # 检查窗口是否有可用空间
            with self.buffer_lock:
                window_used = self._calculate_window_usage()
                if window_used >= self.max_win:
                    self.window_available_event.clear()
                    return

            # 检查是否还有数据发送
            with self.file_lock:
                if self.file_pos >= self.file_size:
                    self.all_data_sent = True
                    return

                # 读取数据
                assert self.file_handle is not None
                read_length = min(MSS, self.file_size - self.file_pos)
                data = self.file_handle.read(read_length)
                self.file_pos += len(data)

                # 获取序列号
                with self.buffer_lock:
                    seq_num = self.next_seq_num
                    self.next_seq_num = (seq_num + len(data)) % MAX_SEQ_NUM

            # 创建并发送数据段
            segment = UrpSegment(seq_num, FLAG_DATA, data)
            self.send_segment(segment, is_new=True)

            # 更新统计信息
            with self.stats_lock:
                self.stats["original_data_sent"] += len(data)

            # 如果这是第一个未确认的段，启动定时器
            with self.buffer_lock:
                if len(self.send_buffer) == 1:
                    self.timer.start()

    def _calculate_window_usage(self):
        """计算当前窗口使用量"""
        total_bytes = 0
        for segment, _ in self.send_buffer.values():
            if segment and segment.flags == FLAG_DATA:
                total_bytes += len(segment.data)

        return total_bytes

    # ========================================
    # Event 2: 超时事件
    # ========================================
    def handle_timeout(self):
        """处理超时，重传最老的段"""
        with self.buffer_lock:
            if not self.send_buffer:
                return

            self.retrans_oldest()

    def checktimeout(self):
        """超时检查 (仅用于连接建立和关闭)"""
        with self.buffer_lock:
            if not self.send_buffer:
                return

            # 检查是否超时
            if time.time() - self.send_buffer[self.base][1] >= self.rto:
                print("连接或关闭阶段检测到超时，开始重传SYN/FIN")
                self.retrans_oldest()

    def retrans_oldest(self, retrans_type="timeout_retrans"):
        """重传最老的段

        Args:
            retrans_type (str): timeout or fast_retrans
        """
        print("开始重传...")
        with self.buffer_lock:
            if not self.send_buffer:
                return

            print(f"当前窗口序列号起点: {self.base}")
            segment, _ = self.send_buffer[self.base]

            if segment:
                print(f"重传序列号: {segment.seq_num}")
                self.send_segment(segment)

                with self.stats_lock:
                    self.stats[retrans_type] += 1

                # 更新发送时间
                self.send_buffer[self.base] = (segment, time.time())

    # ========================================
    # Event 3: ACK接收
    # ========================================
    def receive_ack_thread(self):
        """接收ACK线程"""
        self.sock.settimeout(0.01)

        while self.running:
            self.receive_ack()

    def check_for_ack(self, timeout):
        """检查是否有ACK到达 (仅用于连接建立和关闭)"""
        self.sock.settimeout(timeout)

        return self.receive_ack()

    def receive_ack(self):
        """接收ACK段"""
        try:
            data, addr = self.sock.recvfrom(2048)

            # PLC 处理
            processed_data, status = self.plc.process_bk(data)

            # 解析ACK段
            ack_segment = UrpSegment.unpack(data)
            if not ack_segment:
                return

            print(f"接收到ACK: {ack_segment.seq_num}")

            if status == "cor":
                print(f"ACK损坏: {ack_segment.seq_num}")
                with self.stats_lock:
                    self.stats["corrupted_acks_discarded"] += 1

            elif status == "ok":
                # 验证校验和
                if not ack_segment.verify_checksum():
                    status = "cor"
                    with self.stats_lock:
                        self.stats["corrupted_acks_discarded"] += 1
                else:
                    print("ACK校验完成，开始处理...")
                    self.process_ack(ack_segment)

            else:
                print(f"该段被丢弃: {ack_segment.seq_num}")

            with self.log_lock:
                self.log.log_segment("rcv", status, ack_segment, 0)

            return status == "ok"

        except socket.timeout:
            pass

        except Exception as e:
            print(f"Error: {e}")

    def process_ack(self, segment: UrpSegment):
        """处理接收到的ACK段"""
        ack_num = segment.seq_num

        with self.state_lock:
            current_state = self.state

        # 根据状态处理ACK
        if current_state == STATE_SYN_SENT:
            # Expected ACK = ISN + 1
            expected_ack = (self.isn + 1) % MAX_SEQ_NUM

            if ack_num == expected_ack:
                with self.state_lock:
                    self.state = STATE_ESTABLISHED
                with self.buffer_lock:
                    self.send_buffer.clear()  # 清空缓冲区
                    self.base = expected_ack

        elif self.state == STATE_ESTABLISHED or self.state == STATE_CLOSING:
            # 检查是否是新的ACK
            if self._is_new_ack(ack_num):
                with self.buffer_lock:
                    # 滑动窗口到当前ack (也是下一个要发送的seg的起始位)
                    self.base = ack_num

                    # 移除已确认的段
                    segments_remove = []
                    for seq in self.send_buffer.keys():
                        if self._is_before(seq, ack_num):
                            segments_remove.append(seq)

                    for seq in segments_remove:
                        print(f"删除发送缓冲区内序列号: {seq}")

                        del self.send_buffer[seq]

                        self._print_buffer()

                    has_unacked = len(self.send_buffer) > 0

                # 重置计数器
                with self.ack_lock:
                    self.dup_ack_count = 0
                    self.last_ack_num = ack_num

                # 重启定时器
                if has_unacked:
                    print("收到新ACK，重启定时器")
                    self.timer.restart()
                else:
                    # 所有的段均已确认，停止定时器
                    print("所有的段均已确认，停止定时器")
                    self.timer.stop()

                # 空出可用空间
                self.window_available_event.set()

            else:
                # 是重复ACK
                with self.ack_lock:
                    print("收到重复ACK")
                    if ack_num == self.last_ack_num:
                        self.dup_ack_count += 1

                        with self.stats_lock:
                            self.stats["dup_acks_received"] += 1

                        # 快速重传
                        if self.dup_ack_count == 3:
                            print("快速重传")
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
        """发送段 (通过PLC模块)"""
        segment_bytes = segment.pack()

        # 通过PLC处理
        processed_data, status = self.plc.process_fd(segment_bytes)

        # 记录日志
        with self.log_lock:
            self.log.log_segment("snd", status, segment, len(segment.data))

        # 如果没被丢弃，成功发送
        if status != "drp":
            assert processed_data is not None
            self.sock.sendto(processed_data, ("127.0.0.1", self.receiver_port))
            print(f"发送段序列号: {segment.seq_num}")

        # 如果是新数据段或者重传，加入缓冲区
        if segment.flags != FLAG_ACK:
            with self.buffer_lock:
                self.send_buffer[segment.seq_num] = (segment, time.time())

        # 更新统计信息
        with self.stats_lock:
            if is_new:
                self.stats["original_segments_sent"] += 1

            self.stats["total_segments_sent"] += 1

            if segment.flags == FLAG_DATA:
                self.stats["total_data_sent"] += len(segment.data)

    def write_stats(self):
        """写入统计信息"""
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
        """判断是否是新的ACK"""
        with self.buffer_lock:
            return self._is_before(self.base, ack_num)

    def _is_before(self, seq1, seq2):
        """判断seq1是否比seq2旧

        Returns:
            bool: True - seq1比seq2旧
        """
        diff = (seq2 - seq1) % MAX_SEQ_NUM
        return 0 < diff <= MAX_SEQ_NUM // 2

    def _print_buffer(self):
        print("当前缓冲区: ", end="")
        for seq_num in self.send_buffer.keys():
            print(f"{seq_num}, ", end="")
        print()


# ========================================
# 主函数
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
