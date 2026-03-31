from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from src.baseline.scan import BaselineScanner
from src.metrics.metrics import QueryMetrics
from src.query.predicates import Predicate
from src.storage.table import Table


@dataclass
class ExecutionResult:
    row_ids: list[int]
    metrics: QueryMetrics


class QueryExecutor:
    """
    Executes simple conjunctive queries using the baseline scanner.

    For now, this means:
    - scan each predicate against its target column
    - collect matching row IDs
    - intersect the row ID sets for AND semantics
    """

    def __init__(self) -> None:
        self.baseline_scanner = BaselineScanner()

    def execute_conjunctive_baseline(
        self,
        table: Table,
        predicates: list[Predicate],
    ) -> ExecutionResult:
        """
        Execute a query of the form:

            predicate_1 AND predicate_2 AND ... AND predicate_n

        using only the baseline scan path.

        Returns:
            ExecutionResult containing:
            - sorted matching row IDs
            - accumulated query metrics
        """

        if not predicates:
            row_ids = list(range(table.num_rows))
            return ExecutionResult(
                row_ids=row_ids,
                metrics=QueryMetrics(),
            )

        partial_results: list[set[int]] = []
        total_metrics = QueryMetrics()

        for predicate in predicates:
            column = table.get_column(predicate.column)
            result = self.baseline_scanner.scan_column(column, predicate)

            partial_results.append(set(result.row_ids))
            total_metrics += result.metrics

        final_row_ids = sorted(reduce(lambda left, right: left & right, partial_results))

        return ExecutionResult(
            row_ids=final_row_ids,
            metrics=total_metrics,
        )