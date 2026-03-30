from __future__ import annotations

from src.data_gen import DatasetConfig, SyntheticDataGenerator
from src.query.executor import QueryExecutor
from src.query.predicates import Between, Eq


def main() -> None:
    generator = SyntheticDataGenerator(DatasetConfig(num_rows=1000, segment_size=128, seed=42))
    table = generator.build_demo_table()
    executor = QueryExecutor()

    queries = [
        [Eq(column="status", target="ACTIVE")],
        [Between(column="customer_id", lower=100, upper=500)],
        [Eq(column="status", target="ACTIVE"), Between(column="amount", lower=100, upper=300)],
    ]

    for i, predicates in enumerate(queries, start=1):
        result = executor.execute_conjunctive_baseline(table, predicates)
        print(f"Query {i}: matched={len(result.row_ids)} metrics={result.metrics}")


if __name__ == "__main__":
    main()
