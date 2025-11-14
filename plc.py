"""
PLC 模块
"""

import random


class PlcModule:
    """模拟不可靠信道的包丢失和损失

    Attributes:
        flp (float): 前向丢失率
        rlp (float): 后向丢失率
        fcp (float): 前向损坏率
        rcp (float): 后向损坏率
        fd_dropped (int): 前向丢失计数器
        bk_dropped (int): 后向丢失计数器
        fd_corrupted (int): 前向损失计数器
        bk_corrupted (int): 后向损失计数器
    """

    def __init__(self, flp, rlp, fcp, rcp) -> None:
        self.flp = flp
        self.rlp = rlp
        self.fcp = fcp
        self.rcp = rcp

        self.fwd_dropped = 0
        self.rev_dropped = 0
        self.fwd_corrupted = 0
        self.rev_corrupted = 0

    def process_fd(self, data):
        """处理前向包

        Sender --> PLC --> Receiver

        Returns:
            bytes: Segment data
            str: status, include: ok, drp (dropped), cor (corrupted)
        """

        if random.random() < self.flp:
            self.fwd_dropped += 1
            return None, "drp"

        if random.random() < self.fcp:
            self.fwd_corrupted += 1
            corrupted_data = self._corrupt_segment(data)
            return corrupted_data, "cor"

        return data, "ok"

    def process_bk(self, data):
        """处理反向包

        Sender <-- PLC <-- Receiver

        Returns:
            bytes: ACK data
            str: status, include: ok, drp, cor
        """

        if random.random() < self.rlp:
            self.rev_dropped += 1
            return None, "drp"

        if random.random() < self.rcp:
            self.rev_corrupted += 1
            corrupted_data = self._corrupt_segment(data)
            return corrupted_data, "cor"

        return data, "ok"

    @staticmethod
    def _corrupt_segment(data):
        """损坏段数据

        跳过前4 bytes，随机选择一个字节并随机翻转其中一个位
        """

        if len(data) <= 4:
            return data

        data = bytearray(data)

        byte_pos = random.randint(4, len(data) - 1)
        bit_pos = random.randint(0, 7)
        data[byte_pos] ^= 1 << bit_pos

        return bytes(data)
