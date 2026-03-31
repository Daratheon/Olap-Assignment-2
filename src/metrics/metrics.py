from __future__ import annotations
from dataclasses import dataclass


@dataclass
class QueryMetrics:
    """
    Stores performance metrics collected during query execution.
    """

    values_scanned: int = 0
    segments_scanned: int = 0
    segments_skipped: int = 0
    false_positives: int = 0
    bytes_used: int = 0

    def __add__(self, other: "QueryMetrics") -> "QueryMetrics":
        """
        Allows two QueryMetrics objects to be added together.

        Example:
        metrics_total = metrics_a + metrics_b
        """
        return QueryMetrics(
            values_scanned=self.values_scanned + other.values_scanned,
            segments_scanned=self.segments_scanned + other.segments_scanned,
            segments_skipped=self.segments_skipped + other.segments_skipped,
            false_positives=self.false_positives + other.false_positives,
            bytes_used=self.bytes_used + other.bytes_used,
        )

    def __iadd__(self, other: "QueryMetrics") -> "QueryMetrics":
        """
        In-place addition for accumulating metrics.
        Supports: total_metrics += result.metrics
        """
        self.values_scanned += other.values_scanned
        self.segments_scanned += other.segments_scanned
        self.segments_skipped += other.segments_skipped
        self.false_positives += other.false_positives
        self.bytes_used += other.bytes_used
        return self