from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Generic, Iterable, TypeVar

from .segment import Segment

T = TypeVar("T")


@dataclass
class Column(Generic[T]):
    name: str
    data_type: str
    values: list[T]
    segment_size: int

    def __post_init__(self) -> None:
        if self.segment_size <= 0:
            raise ValueError("segment_size must be positive")

    @property
    def num_rows(self) -> int:
        return len(self.values)

    @property
    def num_segments(self) -> int:
        return ceil(self.num_rows / self.segment_size) if self.num_rows else 0

    def iter_segments(self) -> Iterable[Segment[T]]:
        for segment_id, start in enumerate(range(0, self.num_rows, self.segment_size)):
            end = min(start + self.segment_size, self.num_rows)
            yield Segment.from_values(segment_id, start, self.values[start:end])
