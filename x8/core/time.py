__all__ = ["Time"]


import time


class Time:
    @staticmethod
    def now() -> float:
        timestamp = time.time()
        return timestamp

    @staticmethod
    def now_ms() -> float:
        timestamp_ms = time.time_ns() / 1_000_000
        return timestamp_ms
