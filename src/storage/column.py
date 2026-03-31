from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Generic, Iterable, TypeVar

from .segment import Segment

T = TypeVar("T")


@dataclass
class Column(Generic[T]):
    """
    Represents a column in the table.

    Stores raw values and exposes the data as logical segments.
    OLAP engines process columns in segments to improve scan efficiency.
    """

    name: str
    data_type: str
    values: list[T]
    segment_size: int

    def __post_init__(self) -> None:
        """
        Runs automatically after dataclass initialization.

        Ensures the segment size is valid.
        """
        if self.segment_size <= 0:
            raise ValueError("segment_size must be positive")

    @property
    def num_rows(self) -> int:
        """
        Total number of rows in this column.
        """
        return len(self.values)

    @property
    def num_segments(self) -> int:
        """
        Number of segments needed to store the column.

        Example:
        rows = 10
        segment_size = 4

        segments = ceil(10 / 4) = 3
        """
        return ceil(self.num_rows / self.segment_size) if self.num_rows else 0

    def iter_segments(self) -> Iterable[Segment[T]]:
        """
        Generates segments for the column.

        Instead of storing all segments permanently, we generate them
        when needed during scans.

        Example:
        values = [10,20,30,40,50,60]
        segment_size = 3

        yields:

        Segment 0 → [10,20,30]
        Segment 1 → [40,50,60]
        """

        for segment_id, start in enumerate(range(0, self.num_rows, self.segment_size)):

            end = min(start + self.segment_size, self.num_rows)

            yield Segment.from_values(
                segment_id,
                start,
                self.values[start:end],
            )