from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.storage.column import Column


@dataclass
class Run:
    value: object
    length: int


@dataclass
class RLEQueryResult:
    row_ids: list[int]
    metrics: QueryMetrics


class RLEColumn:
    """
    Run-Length Encoded representation of a column.
    """

    def __init__(self, column: Column) -> None:
        self.column = column
        self.runs: list[Run] = []
        self._build()

    def _build(self) -> None:
        """
        Build the run-length encoding from the column values.
        """
        if not self.column.values:
            return

        current_value = self.column.values[0]
        run_length = 1

        for value in self.column.values[1:]:

            if value == current_value:
                run_length += 1

            else:
                self.runs.append(Run(current_value, run_length))
                current_value = value
                run_length = 1

        self.runs.append(Run(current_value, run_length))

    def lookup_eq(self, target_value) -> RLEQueryResult:
        """
        Equality lookup using runs.
        """
        row_ids: list[int] = []
        current_row = 0
        values_scanned = 0

        for run in self.runs:

            if run.value == target_value:

                for i in range(run.length):
                    row_ids.append(current_row + i)

            values_scanned += 1
            current_row += run.length

        metrics = QueryMetrics(
            values_scanned=values_scanned,
            segments_scanned=0,
            segments_skipped=0,
            false_positives=0,
            bytes_used=self.estimate_bytes_used(),
        )

        return RLEQueryResult(
            row_ids=row_ids,
            metrics=metrics,
        )

    def estimate_bytes_used(self) -> int:
        """
        Estimate storage cost.

        Each run stores:
        - value
        - length
        """
        bytes_per_run = 16
        return len(self.runs) * bytes_per_run