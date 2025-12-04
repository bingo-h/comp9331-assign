"""
URP Timer Module

Provides a single timer function for timeout retransmission mechanisms.
Completely independent, usable by any module requiring a timer.
"""

import threading


class Timer:
    """
    Restartable Single-Use Timer

    Features:
        1. Supports start, stop, and restart operations
        2. Thread-safe
        3. Calls callback function upon timeout
        4. Supports dynamic timeout duration modification

    Usage:
        def on_timeout():
            print(“Timeout occurred!”)

        timer = Timer(timeout=1.0, callback=on_timeout)
        timer.start()  # Start timer
        timer.stop()   # Stop timer
        timer.restart() # Restart timer
    """

    def __init__(self, timeout, callback, auto_restart=False) -> None:
        """
        Initialize Timer

        Args:
            timeout (float): Timeout duration (seconds)
            callback: Callback function invoked upon timeout (no arguments)
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
        """Start the timer"""
        if self._active:
            print("The timer has started. Exit...")
            return

        with self._lock:
            self._active = True
            self._cancel_event.clear()
            self._timer_thread = threading.Thread(
                target=self._timer_worker, daemon=True
            )
            self._timer_thread.start()

    def stop(self):
        """Stop timer"""
        print("Timer stopped...")

        self._active = False
        self._cancel_event.set()

        if self._timer_thread and self._timer_thread.is_alive():
            # Wait after releasing the lock to avoid deadlock.
            thread = self._timer_thread
            self._timer_thread = None

            # Threads waiting outside the lock
            threading.Thread(
                target=lambda: thread.join(timeout=0.1), daemon=True
            ).start()

    def restart(self):
        """Reset timer"""
        with self._lock:
            if self._active:
                self.stop()

        self.start()

    def _timer_worker(self):
        # Wait for timeout, or be interrupted by a cancel event
        cancelled_flag = self._cancel_event.wait(timeout=self.timeout)

        if not cancelled_flag:
            # Timeout Occurred
            with self._lock:
                if self._active:
                    self._active = False

                try:
                    print("Timeout, entering callback function")
                    self.callback()

                except Exception as e:
                    print(f"Timer interrupt service execution exception: {e}")

                finally:
                    if self.auto_restart:
                        print("Restarting timer...")
                        self.restart()
