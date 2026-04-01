from src.compression.mostly import MostlyEncodedColumn
from src.storage.column import Column


def main():
    values = [1, 2, 3, 4, 5, 6, 100, 7, 8, 200]

    column = Column(
        name="numbers",
        data_type="int",
        values=values,
        segment_size=5,
    )

    encoded = MostlyEncodedColumn(column, small_bit_width=4)

    print("inline_values:", encoded.inline_values)
    print("exceptions:", encoded.exceptions)

    eq_result = encoded.lookup_eq(100)
    print("EQ row IDs for 100:", eq_result.row_ids)
    print("EQ metrics:", eq_result.metrics)

    range_result = encoded.lookup_between(5, 110)
    print("BETWEEN row IDs for [5, 110]:", range_result.row_ids)
    print("BETWEEN metrics:", range_result.metrics)

    decoded = encoded.decode_all()
    print("Decoded values:", decoded)


if __name__ == "__main__":
    main()