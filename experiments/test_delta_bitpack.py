from src.compression.delta_bitpack import DeltaBitPackedColumn
from src.storage.column import Column


def main():
    values = [1000, 1002, 1003, 1007, 2000, 2001, 2003, 2008]

    column = Column(
        name="timestamp",
        data_type="int",
        values=values,
        segment_size=4,
    )

    packed = DeltaBitPackedColumn(column)

    print("Packed segments:")
    for segment in packed.packed_segments:
        print(segment)

    eq_result = packed.lookup_eq(1003)
    print("EQ row IDs for 1003:", eq_result.row_ids)
    print("EQ metrics:", eq_result.metrics)

    range_result = packed.lookup_between(1002, 2001)
    print("BETWEEN row IDs for [1002, 2001]:", range_result.row_ids)
    print("BETWEEN metrics:", range_result.metrics)

    decoded = packed.decode_all()
    print("Decoded values:", decoded)


if __name__ == "__main__":
    main()