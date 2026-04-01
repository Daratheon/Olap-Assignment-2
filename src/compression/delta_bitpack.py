from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.storage.column import Column


@dataclass
class PackedSegment:
    """
    Represents one delta-encoded, bit-packed logical segment.

    base_value:
        The first value in the segment.

    deltas:
        Differences from the base value.

    bit_width:
        Minimum number of bits needed to represent the largest delta
        in this segment.
    """

    start_row_id: int
    base_value: int
    deltas: list[int]
    bit_width: int


@dataclass
class DeltaBitPackQueryResult:
    row_ids: list[int]
    metrics: QueryMetrics


class DeltaBitPackedColumn:
    """
    Delta + bit-packed representation for sorted integer/timestamp columns.

    Educational implementation:
    - delta-encode each segment independently
    - compute the minimum bit width needed for that segment's deltas
    - store the logical packed representation
    - reconstruct values only when scanning/querying

    This is intentionally simple and assignment-friendly.
    """

    def __init__(self, column: Column) -> None:
        self.column = column
        self.packed_segments: list[PackedSegment] = []
        self._build()

    def _build(self) -> None:
        """
        Build packed segments from the column.

        For each segment:
        - choose the first value as the base
        - store each value as delta = value - base
        - compute minimum bit width for the largest delta
        """
        for segment in self.column.iter_segments():
            if not segment.values:
                continue

            base_value = int(segment.values[0])
            deltas = [int(value) - base_value for value in segment.values]

            max_delta = max(deltas) if deltas else 0
            bit_width = self._bits_required(max_delta)

            self.packed_segments.append(
                PackedSegment(
                    start_row_id=segment.start_row_id,
                    base_value=base_value,
                    deltas=deltas,
                    bit_width=bit_width,
                )
            )

    def lookup_eq(self, target_value: int) -> DeltaBitPackQueryResult:
        """
        Equality lookup by reconstructing values segment-by-segment.
        """
        row_ids: list[int] = []
        values_scanned = 0

        for packed_segment in self.packed_segments:
            reconstructed = self._reconstruct_segment_values(packed_segment)

            for offset, value in enumerate(reconstructed):
                values_scanned += 1
                if value == target_value:
                    row_ids.append(packed_segment.start_row_id + offset)

        return DeltaBitPackQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=len(self.packed_segments),
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def lookup_between(self, low: int, high: int) -> DeltaBitPackQueryResult:
        """
        Inclusive range lookup by reconstructing values segment-by-segment.
        """
        row_ids: list[int] = []
        values_scanned = 0

        for packed_segment in self.packed_segments:
            reconstructed = self._reconstruct_segment_values(packed_segment)

            for offset, value in enumerate(reconstructed):
                values_scanned += 1
                if low <= value <= high:
                    row_ids.append(packed_segment.start_row_id + offset)

        return DeltaBitPackQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=len(self.packed_segments),
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def decode_all(self) -> list[int]:
        """
        Reconstruct the full original column from all packed segments.
        Useful for correctness checks.
        """
        values: list[int] = []

        for packed_segment in self.packed_segments:
            values.extend(self._reconstruct_segment_values(packed_segment))

        return values

    def _reconstruct_segment_values(self, packed_segment: PackedSegment) -> list[int]:
        """
        Reconstruct original values for one segment from:
        value = base_value + delta
        """
        return [packed_segment.base_value + delta for delta in packed_segment.deltas]

    def _bits_required(self, value: int) -> int:
        """
        Minimum bits needed to store a nonnegative integer.

        Examples:
        0 -> 1 bit
        1 -> 1 bit
        2 -> 2 bits
        3 -> 2 bits
        4 -> 3 bits
        """
        if value <= 0:
            return 1
        return value.bit_length()

    def estimate_bytes_used(self) -> int:
        """
        Educational estimate of storage:

        For each segment:
        - base_value stored as 8 bytes
        - bit_width stored as 4 bytes
        - packed deltas stored using bit_width bits each

        We round packed delta storage up to the nearest byte.
        """
        total_bytes = 0

        for packed_segment in self.packed_segments:
            base_bytes = 8
            bit_width_bytes = 4

            packed_bits = len(packed_segment.deltas) * packed_segment.bit_width
            packed_delta_bytes = (packed_bits + 7) // 8

            total_bytes += base_bytes + bit_width_bytes + packed_delta_bytes

        return total_bytes