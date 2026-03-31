from src.storage.column import Column
from src.indexes.zone_map import ZoneMapScanner
from src.query.predicates import Between


def main():

    # Example column
    timestamps = [
        10, 20, 30, 40,
        100, 110, 120, 130
    ]

    column = Column(
        name="timestamp",
        data_type="int",
        values=timestamps,
        segment_size=4
    )

    scanner = ZoneMapScanner()

    predicate = Between("timestamp", 105, 125)

    result = scanner.scan_column(column, predicate)

    print("Matching row IDs:", result.row_ids)
    print("Metrics:", result.metrics)


if __name__ == "__main__":
    main()