from src.indexes.bit_sliced_index import BitSlicedIndex
from src.storage.column import Column


def main():
    values = [1, 2, 3, 4, 5, 6, 7]

    column = Column(
        name="numbers",
        data_type="int",
        values=values,
        segment_size=4,
    )

    index = BitSlicedIndex(column)

    print("bit_width:", index.bit_width)
    print("bit_slices:")
    for i, bitmap in enumerate(index.bit_slices):
        print(f"bit {i}: {bitmap}")

    eq_result = index.lookup_eq(5)
    print("EQ row IDs for 5:", eq_result.row_ids)
    print("EQ metrics:", eq_result.metrics)

    range_result = index.lookup_between(3, 6)
    print("BETWEEN row IDs for [3, 6]:", range_result.row_ids)
    print("BETWEEN metrics:", range_result.metrics)


if __name__ == "__main__":
    main()