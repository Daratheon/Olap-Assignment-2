from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.storage.column import Column


@dataclass
class BitSlicedQueryResult:
    row_ids: list[int]
    metrics: QueryMetrics


class BitSlicedIndex:
    """
    Bit-sliced index for nonnegative integers.

    We build one bitmap per bit position.
    Example:
        values = [1, 2, 3]
        binary = [001, 010, 011]

    Bit slices:
        bit 0 -> [1, 0, 1]
        bit 1 -> [0, 1, 1]
        bit 2 -> [0, 0, 0]

    This educational implementation supports equality and BETWEEN
    by reconstructing values from slices during lookup.
    """

    def __init__(self, column: Column) -> None:
        self.column = column
        self.num_rows = column.num_rows
        self.max_value = max((int(v) for v in column.values), default=0)
        self.bit_width = self._bits_required(self.max_value)
        self.bit_slices: list[list[int]] = []

        self._build()

    def _build(self) -> None:
        self.bit_slices = [[0] * self.num_rows for _ in range(self.bit_width)]

        for row_id, value in enumerate(self.column.values):
            int_value = int(value)

            for bit_pos in range(self.bit_width):
                self.bit_slices[bit_pos][row_id] = (int_value >> bit_pos) & 1

    def lookup_eq(self, target_value: int) -> BitSlicedQueryResult:
        row_ids: list[int] = []
        values_scanned = 0

        for row_id in range(self.num_rows):
            value = self._reconstruct_value_at_row(row_id)
            values_scanned += 1
            if value == target_value:
                row_ids.append(row_id)

        return BitSlicedQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=0,
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def lookup_between(self, low: int, high: int) -> BitSlicedQueryResult:
        row_ids: list[int] = []
        values_scanned = 0

        for row_id in range(self.num_rows):
            value = self._reconstruct_value_at_row(row_id)
            values_scanned += 1
            if low <= value <= high:
                row_ids.append(row_id)

        return BitSlicedQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=0,
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def _reconstruct_value_at_row(self, row_id: int) -> int:
        value = 0

        for bit_pos, bitmap in enumerate(self.bit_slices):
            if bitmap[row_id]:
                value |= (1 << bit_pos)

        return value

    def _bits_required(self, value: int) -> int:
        if value <= 0:
            return 1
        return value.bit_length()

    def estimate_bytes_used(self) -> int:
        """
        Simple estimate:
        each bit stored as 1 byte in this educational version.
        """
        return self.bit_width * self.num_rows