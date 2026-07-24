"""Thread-safe rate limiter for the parallel send path.

UBKI accepts up to 30 packets/sec; we pace at a configurable ceiling (25 by
default). Even spacing (one slot every `1/rate` seconds) is deliberately
stricter than a token bucket: a bucket permits a burst up to its capacity,
which could momentarily exceed the bureau's hard per-second limit right after
an idle stretch. Even spacing never lets more than `rate` starts happen in any
one-second window, whatever the worker-pool size.

The clock and sleep are injectable so tests can assert the pacing math
deterministically instead of relying on wall-clock timing.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class RateLimiter:
    def __init__(
        self,
        rate_per_sec: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        self._interval = 1.0 / rate_per_sec
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self) -> None:
        """Block until this caller may start its request. Reserves the next
        evenly-spaced slot under the lock, then sleeps outside it so other
        threads can reserve their own slots concurrently."""
        with self._lock:
            now = self._monotonic()
            slot = self._next_slot if self._next_slot > now else now
            self._next_slot = slot + self._interval
        wait = slot - self._monotonic()
        if wait > 0:
            self._sleep(wait)
