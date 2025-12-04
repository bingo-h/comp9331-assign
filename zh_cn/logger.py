"""
日志模块

记录日志

Functions:
    TODO
"""

from pathlib import Path
import time

from urp import UrpSegment


class Logger:
    """日志记录"""

    def __init__(self, file_name) -> None:
        self.log_file = open(f"{file_name}", "w")
        self.start_time = None

    def log_segment(self, direction, status, segment: UrpSegment, length):
        if self.start_time is None:
            elapsed = 0.0
            self.start_time = time.time()
        else:
            elapsed = (time.time() - self.start_time) * 1000

        segment_type = segment.get_seg_type()
        log_entry = f"{direction:3s} {status:3s} {elapsed:7.2f} {segment_type:4s} {segment.seq_num:5d} {length:4d}\n"
        self.log_file.write(log_entry)
        self.log_file.flush()
