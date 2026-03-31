from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Sequence, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Segment(Generic[T]):
    """Immutable fixed-width logical segment of a column."""

    segment_id: int
    start_row_id: int
    values: tuple[T, ...]

    @property
    def end_row_id_exclusive(self) -> int:
        return self.start_row_id + len(self.values)

    @property
    def row_ids(self) -> range:
        return range(self.start_row_id, self.end_row_id_exclusive)

    @classmethod
    def from_values(
        cls,
        segment_id: int,
        start_row_id: int,
        values: Sequence[T],
    ) -> "Segment[T]":
        return cls(
            segment_id=segment_id,
            start_row_id=start_row_id,
            values=tuple(values),
        )

    def scan(self, predicate: Callable[[T], bool]) -> list[int]:
        """Return global row IDs whose values satisfy the predicate."""
        matches: list[int] = []

        for offset, value in enumerate(self.values):
            if predicate(value):
                matches.append(self.start_row_id + offset)

        return matches