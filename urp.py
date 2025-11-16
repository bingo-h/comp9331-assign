"""
URP Protocol Module
"""

import struct

# Constants definition
HEADER_SIZE = 6  # Header size: 6 bytes

# Flags
FLAG_ACK = 0b00000100
FLAG_SYN = 0b00000010
FLAG_FIN = 0b00000001
FLAG_DATA = 0b00000000


class UrpSegment:
    """URP Protocol Segment Type

    Attributes:
        seq_num (int): Sequence number (16 bits)
        flags (int): Control flags (3 bits)
        data (bytes): Data portion
        checksum (int): Checksum (16 bits)
    """

    def __init__(self, seq_num=0, flags=0, data=b"") -> None:
        self.seq_num = seq_num
        self.flags = flags
        self.data = data
        self.checksum = 0

    def pack(self):
        """Pack data into a byte stream

        Construct segment header:
            Sequence number (2 bytes)
            Reserved bits (12 bits) + Control flag bits (3 bits)
            Checksum (2 bytes)
            Total: 6 bytes

        Returns:
            bytes: Byte stream data segment
        """
        reserved_flags = self.flags & 0b00000111  # Only retain the lower 3 digits
        header = struct.pack("!HHH", self.seq_num, reserved_flags, 0)

        # Calculate the checksum
        segment = header + self.data
        self.checksum = self._calculate_checksum(segment)

        # Rebuild header
        header = struct.pack("!HHH", self.seq_num, reserved_flags, self.checksum)

        return header + self.data

    @staticmethod
    def unpack(data: bytes):
        """Unpacking from Byte Stream

        Returns:
            urp_segment: URP protocol segment
        """
        if len(data) < HEADER_SIZE:
            return None

        # Parsing Header
        seq_num, reserved_flags, checksum = struct.unpack("!HHH", data[:HEADER_SIZE])
        flags = reserved_flags & 0x07
        payload = data[HEADER_SIZE:]

        segment = UrpSegment(seq_num, flags, payload)
        segment.checksum = checksum

        return segment

    @staticmethod
    def _calculate_checksum(data):
        """Calculate checksum on a per-character basis

        Formula:
            checksum = (checksum & 0xFFFF) + (checksum >> 16)
            Where checksum & 0xFFFF extracts the lower 16 bits,
            and checksum >> 16 captures the carry.
            Ultimately, checksum will only be less than 16 bits in each iteration.
        """

        # Handling Odd Data
        if len(data) % 2 == 1:
            data += b"\x00"

        total_sum = 0
        for i in range(0, len(data), 2):
            word = (data[i] << 8) + data[i + 1]
            total_sum += word
            total_sum = (total_sum & 0xFFFF) + (total_sum >> 16)

        return ~total_sum & 0xFFFF

    def verify_checksum(self):
        """Verify checksum"""

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
