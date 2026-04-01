from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.storage.column import Column


@dataclass
class MostlyQueryResult:
    row_ids: list[int]
    metrics: QueryMetrics


class MostlyEncodedColumn:
    """
    Mostly encoding for integer columns.

    Most values are stored directly in a small fixed width.
    Values that exceed that width are stored in an exceptions map.

    Example with small_bit_width = 4:
        representable inline values: 0..15

    If a value is > 15, store a marker inline and place the real value
    into exceptions[row_id].
    """

    def __init__(self, column: Column, small_bit_width: int = 4) -> None:
        self.column = column
        self.small_bit_width = small_bit_width
        self.max_inline_value = (1 << small_bit_width) - 1
        self.inline_values: list[int] = []
        self.exceptions: dict[int, int] = {}

        self._build()

    def _build(self) -> None:
        marker = self.max_inline_value

        for row_id, value in enumerate(self.column.values):
            int_value = int(value)

            if int_value < self.max_inline_value:
                self.inline_values.append(int_value)
            else:
                self.inline_values.append(marker)
                self.exceptions[row_id] = int_value

    def lookup_eq(self, target_value: int) -> MostlyQueryResult:
        row_ids: list[int] = []
        values_scanned = 0

        for row_id in range(len(self.inline_values)):
            values_scanned += 1
            value = self._decode_value_at_row(row_id)

            if value == target_value:
                row_ids.append(row_id)

        return MostlyQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=0,
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def lookup_between(self, low: int, high: int) -> MostlyQueryResult:
        row_ids: list[int] = []
        values_scanned = 0

        for row_id in range(len(self.inline_values)):
            values_scanned += 1
            value = self._decode_value_at_row(row_id)

            if low <= value <= high:
                row_ids.append(row_id)

        return MostlyQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=0,
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def decode_all(self) -> list[int]:
        return [self._decode_value_at_row(row_id) for row_id in range(len(self.inline_values))]

    def _decode_value_at_row(self, row_id: int) -> int:
        if row_id in self.exceptions:
            return self.exceptions[row_id]
        return self.inline_values[row_id]

    def estimate_bytes_used(self) -> int:
        """
        Educational estimate:
        - inline values stored at small_bit_width each, rounded to bytes total
        - each exception stores row_id + value, 8 bytes each
        """
        inline_bits = len(self.inline_values) * self.small_bit_width
        inline_bytes = (inline_bits + 7) // 8

        exception_bytes = len(self.exceptions) * 16
        return inline_bytes + exception_bytes