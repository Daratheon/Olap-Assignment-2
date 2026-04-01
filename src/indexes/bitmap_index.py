from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.storage.column import Column


@dataclass
class BitmapQueryResult:
    row_ids: list[int]
    metrics: QueryMetrics


class BitmapIndex:
    """
    Equality-encoded bitmap index for a low-cardinality column.

    For each distinct value in the column, we build a bitmap of length num_rows.
    A 1 means the row contains that value; a 0 means it does not.
    """

    def __init__(self, column: Column) -> None:
        self.column = column
        self.num_rows = column.num_rows
        self.bitmaps: dict[object, list[int]] = {}
        self._build()

    def _build(self) -> None:
        distinct_values = sorted(set(self.column.values))

        for value in distinct_values:
            self.bitmaps[value] = [0] * self.num_rows

        for row_id, value in enumerate(self.column.values):
            self.bitmaps[value][row_id] = 1

    def lookup_eq(self, value) -> BitmapQueryResult:
        """
        Return matching row IDs for equality predicate: column = value
        """
        bitmap = self.bitmaps.get(value, [0] * self.num_rows)
        row_ids = [row_id for row_id, bit in enumerate(bitmap) if bit == 1]

        metrics = QueryMetrics(
            values_scanned=0,
            segments_scanned=0,
            segments_skipped=0,
            false_positives=0,
            bytes_used=self.estimate_bytes_used(),
        )

        return BitmapQueryResult(
            row_ids=row_ids,
            metrics=metrics,
        )

    def and_bitmap(self, left: list[int], right: list[int]) -> list[int]:
        """
        Bitwise AND of two equal-length bitmaps.
        """
        return [a & b for a, b in zip(left, right)]

    def or_bitmap(self, left: list[int], right: list[int]) -> list[int]:
        """
        Bitwise OR of two equal-length bitmaps.
        """
        return [a | b for a, b in zip(left, right)]

    def not_bitmap(self, bitmap: list[int]) -> list[int]:
        """
        Bitwise NOT of a bitmap.
        """
        return [1 - bit for bit in bitmap]

    def bitmap_to_row_ids(self, bitmap: list[int]) -> list[int]:
        """
        Convert a bitmap into matching row IDs.
        """
        return [row_id for row_id, bit in enumerate(bitmap) if bit == 1]

    def get_bitmap(self, value) -> list[int]:
        """
        Return the raw bitmap for a value.
        If the value does not exist, return an all-zero bitmap.
        """
        return self.bitmaps.get(value, [0] * self.num_rows)

    def estimate_bytes_used(self) -> int:
        """
        Simple educational storage estimate.

        We store each bitmap entry as 1 byte for now.
        This is not bit-packed; it is intentionally simple.
        """
        num_distinct = len(self.bitmaps)
        return num_distinct * self.num_rows