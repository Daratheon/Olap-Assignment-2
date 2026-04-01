from __future__ import annotations

from dataclasses import dataclass

from src.baseline.scan import ScanResult
from src.metrics.metrics import QueryMetrics
from src.query.predicates import Between, Eq, Predicate
from src.storage.column import Column


@dataclass
class ImprintGroup:
    start_row_id: int
    values: list[int]
    bitmask: int


class ColumnImprints:
    """
    Approximate pruning structure using coarse range bins.

    We partition the column domain into bins.
    Each row group stores a bitmask telling us which bins appear in that group.

    Queries first test the query against the bitmask.
    If the bitmask says "impossible", skip the group.
    Otherwise scan the base values in that group to verify.
    """

    def __init__(self, column: Column, group_size: int = 4, num_bins: int = 8) -> None:
        self.column = column
        self.group_size = group_size
        self.num_bins = num_bins
        self.min_value = min((int(v) for v in column.values), default=0)
        self.max_value = max((int(v) for v in column.values), default=0)
        self.groups: list[ImprintGroup] = []

        self._build()

    def _build(self) -> None:
        values = [int(v) for v in self.column.values]

        for start in range(0, len(values), self.group_size):
            group_values = values[start:start + self.group_size]
            bitmask = 0

            for value in group_values:
                bitmask |= (1 << self._bin_for_value(value))

            self.groups.append(
                ImprintGroup(
                    start_row_id=start,
                    values=group_values,
                    bitmask=bitmask,
                )
            )

    def scan(self, predicate: Predicate) -> ScanResult:
        matched_row_ids: list[int] = []
        values_scanned = 0
        groups_scanned = 0
        groups_skipped = 0
        false_positives = 0

        query_mask = self._query_mask(predicate)

        for group in self.groups:
            if query_mask is not None and (group.bitmask & query_mask) == 0:
                groups_skipped += 1
                continue

            groups_scanned += 1
            group_had_candidate = False
            group_had_match = False

            for offset, value in enumerate(group.values):
                if query_mask is None or self._value_might_match(value, predicate):
                    group_had_candidate = True

                values_scanned += 1
                if predicate.matches(value):
                    matched_row_ids.append(group.start_row_id + offset)
                    group_had_match = True

            if group_had_candidate and not group_had_match:
                false_positives += 1

        return ScanResult(
            row_ids=matched_row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=groups_scanned,
                segments_skipped=groups_skipped,
                false_positives=false_positives,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def _bin_for_value(self, value: int) -> int:
        if self.max_value == self.min_value:
            return 0

        normalized = (value - self.min_value) / (self.max_value - self.min_value)
        bin_index = int(normalized * self.num_bins)

        if bin_index >= self.num_bins:
            bin_index = self.num_bins - 1

        return bin_index

    def _query_mask(self, predicate: Predicate) -> int | None:
        if isinstance(predicate, Eq):
            return 1 << self._bin_for_value(int(predicate.value))

        if isinstance(predicate, Between):
            low_bin = self._bin_for_value(int(predicate.low))
            high_bin = self._bin_for_value(int(predicate.high))

            mask = 0
            for bin_index in range(low_bin, high_bin + 1):
                mask |= (1 << bin_index)
            return mask

        return None

    def _value_might_match(self, value: int, predicate: Predicate) -> bool:
        return predicate.matches(value)

    def estimate_bytes_used(self) -> int:
        """
        Simple estimate:
        each group stores one integer bitmask + start row id.
        """
        bytes_per_group = 16
        return len(self.groups) * bytes_per_group