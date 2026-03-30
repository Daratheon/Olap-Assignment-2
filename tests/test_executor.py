from src.query.executor import QueryExecutor
from src.query.predicates import Between, Eq
from src.storage.column import Column
from src.storage.table import Table


def test_conjunctive_execution_intersects_matches() -> None:
    table = Table(
        name="t",
        columns={
            "status": Column("status", "string", ["A", "B", "A", "A"], 2),
            "amount": Column("amount", "int", [10, 20, 30, 40], 2),
        },
    )

    executor = QueryExecutor()
    result = executor.execute_conjunctive_baseline(
        table,
        [Eq(column="status", target="A"), Between(column="amount", lower=25, upper=45)],
    )

    assert result.row_ids == [2, 3]
