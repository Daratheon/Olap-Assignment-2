import config # to make src available for import

from src.storage.column import Column
from src.storage.table import Table
from src.query.executor import QueryExecutor
from src.query.predicates import Eq, Between
from src.baseline.scan import BaselineScanner
from src.indexes.zone_map import ZoneMapScanner

import random


def dataset_low_cardinality(n=1000):
    values = ["A", "B", "C", "D"]
    return [random.choice(values) for _ in range(n)]


def dataset_sorted_integers(n=1000):
    return list(range(n))


def dataset_random_integers(n=1000):
    return [random.randint(0, 10000) for _ in range(n)]


def dataset_timestamps(n=1000):
    base = 100000
    return [base + i for i in range(n)]


def run_dataset(name, values):

    print(f"\n=== DATASET: {name} ===")

    column = Column(
        name="value",
        data_type="int",
        values=values,
        segment_size=128
    )

    table = Table(
        name="test_table",
        columns={"value": column}
    )

    executor = QueryExecutor()
    baseline = BaselineScanner()
    zone_map = ZoneMapScanner()

    queries = [
        Eq("value", values[len(values)//2]),
        Between("value", values[100], values[200]),
    ]

    for query in queries:

        print(f"\nQuery: {query}")

        base_result = baseline.scan_column(column, query)
        print("Baseline:", base_result.metrics)

        zm_result = zone_map.scan_column(column, query)
        print("ZoneMap:", zm_result.metrics)


def main():

    datasets = {
        "Low Cardinality": dataset_low_cardinality(),
        "Sorted Integers": dataset_sorted_integers(),
        "Random Integers": dataset_random_integers(),
        "Timestamps": dataset_timestamps(),
    }

    for name, data in datasets.items():
        run_dataset(name, data)


if __name__ == "__main__":
    main()
