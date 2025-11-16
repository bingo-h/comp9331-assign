"""
URP定时器模块

提供单一定时器功能，用于超时重传机制
完全独立，可被任何需要定时器的模块使用
"""

import threading
import time


class Timer:
    """
    可重启的单次定时器

    Features：
        1. 支持启动、停止、重启
        2. 线程安全
        3. 超时时调用回调函数
        4. 支持动态修改超时时间

    Usage：
        def on_timeout():
            print("超时了！")

        timer = Timer(timeout=1.0, callback=on_timeout)
        timer.start()  # 启动定时器
        timer.stop()   # 停止定时器
        timer.restart() # 重启定时器
    """

    def __init__(self, timeout, callback, auto_restart=False) -> None:
        """
        初始化定时器

        Args：
            timeout (float): 超时时间（秒）
            callback: 超时时调用的回调函数（无参数）
        """
        self.timeout = timeout
        self.callback = callback
        self.auto_restart = auto_restart

        # Thread control
        self._lock = threading.RLock()
        self._timer_thread = None
        self._active = False
        self._cancel_event = threading.Event()

    def start(self):
        """启动定时器"""
        if self._active:
            print("定时器已启动，退出...")
            return

        with self._lock:
            self._active = True
            self._cancel_event.clear()
            self._timer_thread = threading.Thread(
                target=self._timer_worker, daemon=True
            )
            self._timer_thread.start()

    def stop(self):
        """停止定时器"""
        print("定时器停止中...")

        with self._lock:
            if self._active:
                self._active = False
                self._cancel_event.set()

            if self._timer_thread and self._timer_thread.is_alive():
                # 释放锁后等待，避免死锁
                thread = self._timer_thread
                self._timer_thread = None

                # 在锁外等待线程
                threading.Thread(
                    target=lambda: thread.join(timeout=0.1), daemon=True
                ).start()

    def restart(self):
        """重启定时器"""
        with self._lock:
            if self._active:
                self.stop()

        self.start()

    def _timer_worker(self):
        # 等待超时时间，或者被取消事件打断
        cancelled_flag = self._cancel_event.wait(timeout=self.timeout)

        if not cancelled_flag:
            # 超时发生
            with self._lock:
                if self._active:
                    self._active = False

                try:
                    print("超时，进入回调函数")
                    self.callback()

                except Exception as e:
                    print(f"定时器中断服务执行异常: {e}")

                finally:
                    if self.auto_restart:
                        print("定时器重启中...")
                        self.restart()
