from src.compression.rle import RLEColumn
from src.storage.column import Column


def main():

    values = [5,5,5,5,5,7,7,7,9,9]

    column = Column(
        name="numbers",
        data_type="int",
        values=values,
        segment_size=5
    )

    rle = RLEColumn(column)

    result = rle.lookup_eq(7)

    print("Row IDs:", result.row_ids)
    print("Metrics:", result.metrics)


if __name__ == "__main__":
    main()