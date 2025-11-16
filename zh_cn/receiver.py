"""
接收模块

单线程处理收到的数据

Functions:
    TODO
"""

import sys
import socket
import threading
import time

from urp import *
from logger import Logger
from timer import Timer

# ========================================
# 常量定义
# ========================================
# 状态常量
STATE_CLOSED = 0
STATE_LISTEN = 1
STATE_ESTABLISHED = 2
STATE_TIME_WAIT = 3

# 其他常量
MAX_SEQ_NUM = 65536  # 序列号最大值: 2^16
MSS = 1000  # 数据段最大值
MSL = 1  # 1s
TIME_WAIT_DURATION = 2 * MSL  # 等待2s


class Receiver:
    """URP接收端

    Attributes:
        receiver_port (int): 接收端口
        recv_buffer (dict): 接收缓冲区 {seq_num: segment_data}
    """

    def __init__(self, receiver_port, sender_port, file_name, max_win) -> None:
        self.server_ip = "127.0.0.1"
        self.receiver_port = receiver_port
        self.sender_port = sender_port
        self.file_name = file_name
        self.max_win = max_win

        # 创建socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.server_ip, receiver_port))
        self.sock.settimeout(0.1)

        # 状态变量
        self.state = STATE_CLOSED
        self.expected_seq_num = 0

        # 接收缓冲区
        self.recv_buffer = {}

        # 输出文件
        self.output_file = None

        # 日志
        self.start_time = None
        self.log = Logger("receiver_log.txt")

        # 统计信息
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

        # 已接收的段序列号集合，用于检测重复
        self.received_seq_num = set()

        # TIME_WAIT定时器
        self.timer = Timer(timeout=TIME_WAIT_DURATION, callback=self.wait_expired)

        # 运行标志
        self.running = True

    def run(self):
        """接收端主线程"""
        try:
            # 打开输出文件
            self.output_file = open(self.file_name, "wb")

            # 进入LISTEN状态
            print("开始监听")
            self.state = STATE_LISTEN

            # 主循环
            while self.running and self.state != STATE_CLOSED:
                self.receive_segment()
                time.sleep(0.001)

            # 写入统计信息
            self.write_stats()

        except Exception as e:
            print(f"Error (Receiver): {e}")

        finally:
            self.cleanup()

    def receive_segment(self):
        """接收并处理段"""
        try:
            data, addr = self.sock.recvfrom(2048)

            # 记录开始时间
            if self.start_time is None:
                self.start_time = time.time()

            # 解析段
            segment = UrpSegment.unpack(data)
            if not segment:
                return

            print("接收到信息...")

            self.stats["total_segments_received"] += 1

            # 验证校验和
            if not segment.verify_checksum():
                self.log.log_segment("rcv", "cor", segment, len(segment.data))
                self.stats["corrupted_segments_discarded"] += 1
                print(f"数据段损坏: {self.stats['corrupted_segments_discarded']}")
                return

            # 记录日志 (正常接收)
            self.log.log_segment("rcv", "ok", segment, len(segment.data))

            # 根据状态处理接收数据
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
        """处理LISTEN状态下收到的数据"""
        if segment.flags & FLAG_SYN:
            # 接收到SYN信号，发送ACK
            print("开始建立连接，发送ACK...")
            self.expected_seq_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(self.expected_seq_num)
            self.state = STATE_ESTABLISHED
            self.stats["original_segments_received"] += 1

    def handle_established(self, segment: UrpSegment):
        """处理ESTABLISHED状态下收到的数据"""
        if segment.flags & FLAG_SYN:
            # 收到重复的SYN，重传ACK
            print("收到重复的SYN，重传ACK")
            self.expected_seq_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(self.expected_seq_num)
            self.stats["duplicate_segments_received"] += 1

        elif segment.flags & FLAG_FIN:
            # 接收到FIN，进入TIME_WAIT
            print("接收到FIN，进入TIME_WAIT")
            ack_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(ack_num)
            self.state = STATE_TIME_WAIT

            # 启动TIME_WAIT定时器
            self.timer.start()

            # 判断是否是重复FIN
            # if segment.seq_num in self.received_seq_num:
            #     self.stats["duplicate_segments_received"] += 1
            # else:
            #     self.stats["original_segments_received"] += 1
            #     self.received_seq_num.add(segment.seq_num)

        elif segment.flags == FLAG_DATA:
            # 接收到DATA段
            print("接收到DATA段")
            self.process_data_segment(segment)

    def handle_wait(self, segment: UrpSegment):
        """处理TIME_WAIT状态下收到的数据"""
        if segment.flags & FLAG_FIN:
            # 重复的FIN，重传ACK
            print("接收到重复FIN，重传ACK")
            ack_num = (segment.seq_num + 1) % MAX_SEQ_NUM
            self.send_ack(ack_num)
            self.stats["duplicate_segments_received"] += 1

    def process_data_segment(self, segment: UrpSegment):
        """处理收到的DATA段"""

        self.stats["total_data_received"] += len(segment.data)

        # 检查是否重复
        is_duplicate = segment.seq_num in self.received_seq_num

        # 更新统计信息
        if not is_duplicate:
            self.stats["original_segments_received"] += 1
            self.stats["original_data_received"] += len(segment.data)
            self.received_seq_num.add(segment.seq_num)
        else:
            self.stats["duplicate_segments_received"] += 1
            self.send_ack(self.expected_seq_num)
            return

        if segment.seq_num == self.expected_seq_num:
            # 如果收到的是当前期望的数据，直接写入文件
            assert self.output_file is not None
            self.output_file.write(segment.data)
            self.expected_seq_num = (
                self.expected_seq_num + len(segment.data)
            ) % MAX_SEQ_NUM

            # 检查缓冲区
            self.flush_buffer()
        else:
            # 乱序，存入缓冲区
            self.recv_buffer[segment.seq_num] = segment.data

        # 发送ACK
        self.send_ack(self.expected_seq_num)

    def flush_buffer(self):
        """从缓冲区写入连续数据"""
        while self.expected_seq_num in self.recv_buffer:
            data = self.recv_buffer[self.expected_seq_num]

            assert self.output_file is not None
            self.output_file.write(data)

            del self.recv_buffer[self.expected_seq_num]

            self.expected_seq_num = (self.expected_seq_num + len(data)) % MAX_SEQ_NUM

    def send_ack(self, ack_num):
        """发送ACK段"""
        ack_segment = UrpSegment(ack_num, FLAG_ACK, b"")
        segment_data = ack_segment.pack()

        # 发送
        print(f"发送ACK: {ack_num}")
        self.sock.sendto(segment_data, (self.server_ip, self.sender_port))

        # 记录日志
        self.log.log_segment("snd", "ok", ack_segment, 0)

        # 更新统计信息
        self.stats["total_acks_sent"] += 1

        # 检查是否发了重复ACK
        if hasattr(self, "last_ack_sent"):
            if self.last_ack_sent == ack_num:
                self.stats["duplicate_acks_sent"] += 1

        self.last_ack_sent = ack_num

    def wait_expired(self):
        print("接收端关闭中...")
        time.sleep(TIME_WAIT_DURATION)
        self.state = STATE_CLOSED
        self.running = False

    def write_stats(self):
        """写入统计信息"""

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
        """清理资源"""
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
