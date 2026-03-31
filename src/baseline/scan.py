from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.query.predicates import Predicate
from src.storage.column import Column


@dataclass
class ScanResult:
    row_ids: list[int]
    metrics: QueryMetrics


class BaselineScanner:
    """Correctness oracle: scans base values only."""

    def scan_column(self, column: Column, predicate: Predicate) -> ScanResult:
        matched_row_ids: list[int] = []
        values_scanned = 0
        segments_scanned = 0

        for segment in column.iter_segments():
            segments_scanned += 1

            for offset, value in enumerate(segment.values):
                values_scanned += 1

                if predicate.matches(value):
                    matched_row_ids.append(segment.start_row_id + offset)

        metrics = QueryMetrics(
            values_scanned=values_scanned,
            segments_scanned=segments_scanned,
            segments_skipped=0,
            false_positives=0,
        )

        return ScanResult(
            row_ids=matched_row_ids,
            metrics=metrics,
        )