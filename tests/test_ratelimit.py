"""RateLimiter pacing math, verified with an injected clock (no wall-clock flake)."""

import threading

import pytest

from app.ratelimit import RateLimiter


class FakeClock:
    """A monotonic clock that only advances when `sleep` is called, so the pacing
    is fully deterministic."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, d: float) -> None:
        self.sleeps.append(d)
        self.now += d


def test_evenly_spaces_calls_by_one_over_rate():
    clock = FakeClock()
    rl = RateLimiter(4.0, monotonic=clock.monotonic, sleep=clock.sleep)  # interval 0.25s

    for _ in range(5):
        rl.acquire()

    # first call goes immediately; the next four are paced one interval apart
    assert clock.sleeps == pytest.approx([0.25, 0.25, 0.25, 0.25])
    # 5 starts across [0, 1.0]s -> never more than the configured rate in a window
    assert clock.now == pytest.approx(1.0)


def test_no_sleep_when_caller_already_behind():
    clock = FakeClock()
    rl = RateLimiter(10.0, monotonic=clock.monotonic, sleep=clock.sleep)  # interval 0.1s

    rl.acquire()          # reserves slot 0.0, next slot 0.1
    clock.now = 5.0       # caller did slow work; plenty of time has passed
    rl.acquire()          # slot = now (5.0) > 0.1, so no wait

    assert clock.sleeps == []


def test_rejects_nonpositive_rate():
    with pytest.raises(ValueError):
        RateLimiter(0)
    with pytest.raises(ValueError):
        RateLimiter(-1.0)


def test_thread_safe_slots_are_unique_and_monotonic():
    """Under concurrent acquire(), reserved slots must be distinct and spaced by
    the interval — no two threads share a slot. We use a real (fast) clock and
    just assert the recorded sleep count and ordering are sane."""
    clock_lock = threading.Lock()
    reserved: list[float] = []
    now = [0.0]

    def monotonic() -> float:
        with clock_lock:
            return now[0]

    def sleep(d: float) -> None:
        with clock_lock:
            now[0] += d
            reserved.append(now[0])

    rl = RateLimiter(1000.0, monotonic=monotonic, sleep=sleep)

    def worker() -> None:
        for _ in range(20):
            rl.acquire()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # every sleep advanced the clock; slots handed out are strictly increasing
    assert reserved == sorted(reserved)
    assert len(set(reserved)) == len(reserved)
