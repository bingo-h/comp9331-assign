"""
发送模块

这个模块负责发送数据

Functions:
    TODO
"""

import socket
import random
import threading
import time

from plc import PlcModule
from urp import FLAG_SYN, UrpSegment

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

    Attributes:
        sender_port (int): 发送端端口
        receiver_port (int): 接收端端口
        filename (str): 发送文件
        max_win (int): 最大发送窗口
        rto (float): 超时时间
        flp (float): 前向损失率
        rlp (float): 后向丢失率
        fcp (float): 前向损坏率
        rcp (float): 后向损坏率

        plc (PlcModule): Plc模块
        sock (socket): UDP Socket
        state (int): 发送端当前状态
        isn (int): 初始序列号
        next_seq_num (int): 下一段序列号
        send_buffer (dict): 发送缓冲区 {seq_num: (segment_data, send_time)}

        timer_thread ()
        timer_running (bool): 计时器运行标志
        timer_lock

        dup_ack_count (int): 重复ACK计数器
        last_ack_num (int): 上一次接收到的ACK
        log_file (file): 日志文件
        start_time (time): 发送端开始时间
        stats (dict): 统计信息

        file_data (bytes): 文件数据
        file_pos (int): 当前发送位置
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
        self.state = STATE_CLOSED
        self.isn = random.randint(0, MAX_SEQ_NUM - 1)
        self.next_seq_num = (self.isn + 1) % MAX_SEQ_NUM
        self.base = self.next_seq_num

        # 发送缓冲区 (未确认段)
        self.send_buffer = {}

        # 定时器
        self.timer_thread = None
        self.timer_running = False
        self.timer_lock = threading.Lock()

        # 重复ACK计数
        self.dup_ack_count = 0
        self.last_ack_num = 0

        # 日志文件
        self.log_file = open("sender_log.txt", "w")
        self.start_time = None

        # 统计信息
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
        self.file_data = None
        self.file_pos = 0
        self.all_data_sent = False

        # 线程控制
        self.running = True

    def run(self):
        """运行发送端主流程"""
        try:
            # 打开文件
            with open(self.filename, "rb") as file:
                self.file_data = file.read()

            # 开始建立连接
            self.start_time = time.time()
            self.connection_setup()

            # 数据传输
            self.data_transfer()

            # 连接关闭
            self.connection_close()

            # 写入日志文件
            self.write_stats()

        except Exception as e:
            print(f"Error: {e}")

        finally:
            self.cleanup()

    def connection_setup(self):
        """建立连接阶段 (SYN, ACK)"""
        self.state = STATE_SYN_SENT

        # 发送SYN
        syn_segment = UrpSegment(self.isn, FLAG_SYN, b"")
        self.send_seg(syn_segment, is_new=True)

        # 启动定时器
        self.start_timer()

        # 等待ACK
        while self.state == STATE_SYN_SENT:
            self.receive_ack()
            self.checktimeout()
            time.sleep(0.001)

        self.stop_timer()

    def data_transfer(self):
        """数据传输阶段"""
        self.state = STATE_ESTABLISHED

        while self.state == STATE_ESTABLISHED:
            # 发送多个新数据段 (如果窗口允许)
            self.send_data_segments()

            # 接收ACK
            self.receive_ack()

            # 检查超时
            self.checktimeout()

            # 检查是否数据已经发送完: file_data 和 send_buffer 均空
            if self.file_pos >= len(self.file_data) and not self.send_buffer:
                self.state = STATE_CLOSING  # 进入开始关闭连接阶段

            time.sleep(0.001)

    def receive_ack(self):
        """接收ACK段"""
        try:
            data, addr = self.sock.recvfrom(2048)

            # PLC 处理
            processed_data = self.plc.process_bk(data)

            if processed_data is None:
                # 段被丢弃
                ack_segment = UrpSegment.unpack(data)

                if ack_segment:
                    self.log_seg("rcv", "drp", ack_segment, 0)

                return

            # 解析ACK段
            ack_segment = UrpSegment.unpack(processed_data)
            if not ack_segment:
                return

            # 验证校验和
            if not ack_segment.verify_checksum():
                self.log_seg("rcv", "cor", ack_segment, 0)
                self.stats["corrupted_acks_discarded"] += 1
                return

            self.log_seg("rcv", "ok", ack_segment, 0)

            self.process_ack(ack_segment)

        except socket.timeout:
            print("Timeout retran...")

        except Exception as e:
            print(f"Error: {e}")

    def process_ack(self, segment: UrpSegment):
        """处理接收到的ACK段"""
        ack_num = segment.seq_num

        # 根据状态处理ACK
        if self.state == STATE_SYN_SENT:
            # Expected ACK = ISN + 1
            expected_ack = (self.isn + 1) % MAX_SEQ_NUM

            if ack_num == expected_ack:
                self.state = STATE_ESTABLISHED
                self.send_buffer.clear()  # 清空缓冲区
                self.stop_timer()

        elif self.state == STATE_ESTABLISHED or self.state == STATE_CLOSING:
            # 检查是否是新的ACK
            if self._is_new_ack(ack_num):
                # 滑动窗口到当前ack (也是下一个要发送的seg的起始位)
                self.base = ack_num

                # 移除已确认的段
                segments_remove = []
                for seq in self.send_buffer:
                    if self._is_before(seq, ack_num):
                        segments_remove.append(seq)

                for seq in segments_remove:
                    del self.send_buffer[seq]

                # 重置计数器
                self.dup_ack_count = 0
                self.last_ack_num = ack_num

                # 重启定时器
                if self.send_buffer:
                    self.start_timer()
                else:
                    self.stop_timer()

            # 是重复ACK
            else:
                self.dup_ack_count += 1
                self.stats["dup_acks_received"] += 1

                # 快速重传
                if self.dup_ack_count == 3:
                    self.fast_retrans()
                    self.dup_ack_count = 0

        elif self.state == STATE_FIN_WAIT:
            expected_ack = (self.next_seq_num + 1) % MAX_SEQ_NUM

            if ack_num == expected_ack:
                self.state = STATE_CLOSED
                self.send_buffer.clear()
                self.stop_timer()

    def fast_retrans(self):
        """快速重传最老的未确认的段"""

        if not self.send_buffer:
            return

        # 找到最老的未确认段
        oldest_seq = min(
            self.send_buffer.keys(), key=lambda x: self._seq_distance(x, self.base)
        )

    def send_data_segments(self):
        pass

    def send_seg(self, seg, is_new):
        pass

    def fast_retrans(self):
        """快速重传最老的未确认的段"""
        pass

    def connection_close(self):
        pass

    def write_stats(self):
        pass

    def cleanup(self):
        pass

    def start_timer(self):
        pass

    def stop_timer(self):
        pass

    def checktimeout(self):
        pass

    def log_seg(self, direction, status, segment, length):
        pass

    def _is_new_ack(self, ack_num):
        pass

    def _is_before(self, ack_num):
        pass

    def _seq_distance(self, seq1, seq2):
        pass
