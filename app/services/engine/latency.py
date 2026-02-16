from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Deque


@dataclass(slots=True)
class _Series:
    samples_ns: Deque[int]


class LatencyRecorder:
    """
    Lightweight rolling latency recorder.

    Hot-path overhead is bounded by:
    - one modulo check per tick
    - a few perf_counter_ns() calls where instrumentation is placed
    - appending integers to a deque (only on sampled ticks)
    """

    def __init__(self, *, sample_every_n: int = 10, maxlen: int = 2048) -> None:
        self.sample_every_n = max(1, int(sample_every_n))
        self.maxlen = max(16, int(maxlen))

        self._tick = 0
        self._series: dict[str, _Series] = {}
        self._counts: dict[str, int] = {}

    def next_tick_should_sample(self) -> bool:
        self._tick += 1
        return (self._tick % self.sample_every_n) == 0

    def should_sample(self) -> bool:
        return (self._tick % self.sample_every_n) == 0

    def inc(self, key: str, n: int = 1) -> None:
        self._counts[key] = int(self._counts.get(key, 0)) + int(n)

    def add_ns(self, key: str, ns: int) -> None:
        s = self._series.get(key)
        if s is None:
            s = _Series(samples_ns=deque(maxlen=self.maxlen))
            self._series[key] = s
        s.samples_ns.append(int(ns))

    @staticmethod
    def now_ns() -> int:
        return time.perf_counter_ns()

    def snapshot(self) -> dict:
        def pct(sorted_vals: list[int], p: float) -> float:
            if not sorted_vals:
                return 0.0
            if p <= 0:
                return float(sorted_vals[0])
            if p >= 1:
                return float(sorted_vals[-1])
            idx = int(round((len(sorted_vals) - 1) * p))
            return float(sorted_vals[idx])

        out: dict = {
            "sample_every_n": self.sample_every_n,
            "buffer_maxlen": self.maxlen,
            "counts": dict(self._counts),
            "series": {},
        }

        for key, series in self._series.items():
            vals = list(series.samples_ns)
            if not vals:
                continue
            vals.sort()
            n = len(vals)
            s = float(sum(vals))
            out["series"][key] = {
                "n": n,
                "min_us": vals[0] / 1_000.0,
                "avg_us": (s / n) / 1_000.0,
                "max_us": vals[-1] / 1_000.0,
                "p50_us": pct(vals, 0.50) / 1_000.0,
                "p95_us": pct(vals, 0.95) / 1_000.0,
                "p99_us": pct(vals, 0.99) / 1_000.0,
            }

        return out
