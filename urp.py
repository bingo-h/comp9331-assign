"""
URP协议类
"""

import struct

# 常量定义
HEADER_SIZE = 6  # 头部长度6字节

# Flags
FLAG_ACK = 0b00000100
FLAG_SYN = 0b00000010
FLAG_FIN = 0b00000001
FLAG_DATA = 0b00000000


class UrpSegment:
    """URP协议段类
    Attributes:
        seq_num (int): 序列号 (16 bits)
        flags (int): 控制标志位 (3 bits)
        data (bytes): 数据部分
        checksum (int): 校验和 (16 bits)
    """

    def __init__(self, seq_num=0, flags=0, data=b"") -> None:
        self.seq_num = seq_num
        self.flags = flags
        self.data = data
        self.checksum = 0

    def pack(self):
        """将数据打包为字节流

        构造分段header:
            序列号 (2 bytes)
            保留位 (12 bits) + 控制标志位 (3 bits)
            校验和 (2 bytes)
            Total: 6 bytes

        Returns:
            bytes: 字节流数据段
        """
        reserved_flags = self.flags & 0b00000111  # 只保留低3位
        header = struct.pack("!HHH", self.seq_num, reserved_flags, 0)

        # 计算校验和
        segment = header + self.data
        self.checksum = self._calculate_checksum(segment)

        # 重新构造 header
        header = struct.pack("!HHH", self.seq_num, reserved_flags, self.checksum)

        return header + self.data

    @staticmethod
    def unpack(data: bytes):
        """从字节流解包

        Returns:
            urp_segment: URP协议段
        """
        if len(data) < HEADER_SIZE:
            return None

        # 解析头部
        seq_num, reserved_flags, checksum = struct.unpack("!HHH", data[:HEADER_SIZE])
        flags = reserved_flags & 0b00000111
        payload = data[HEADER_SIZE:]

        segment = UrpSegment(seq_num, flags, payload)
        segment.checksum = checksum

        return segment

    @staticmethod
    def _calculate_checksum(data):
        """以字为单位计算校验和

        公式:
            checksum = (checksum & 0xFFFF) + (checksum >> 16)
            其中 checksum & 0xFFFF 是为了获取低16位，
            checksum >> 16 是为了获取进位。
            最终每个循环 checksum 都只会是小于16位。
        """

        # 处理奇数数据
        if len(data) % 2 == 1:
            data += b"\x00"

        total_sum = 0
        for i in range(0, len(data), 2):
            word = (data[i] << 8) + data[i + 1]
            total_sum += word
            total_sum = (total_sum & 0xFFFF) + (total_sum >> 16)

        return ~total_sum & 0xFFFF

    def verify_checksum(self):
        """验证校验和"""

        reserved_flags = self.flags & 0b00000111
        header = struct.pack("!HHH", self.seq_num, reserved_flags, 0)

        checksum = self._calculate_checksum(header + self.data)

        return checksum == self.checksum

    def get_seg_type(self):
        if self.flags & FLAG_ACK:
            return "ACK"
        elif self.flags & FLAG_SYN:
            return "SYN"
        elif self.flags & FLAG_FIN:
            return "FIN"
        else:
            return "DATA"
