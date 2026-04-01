from src.indexes.column_imprints import ColumnImprints
from src.query.predicates import Between, Eq
from src.storage.column import Column


def main():
    values = [10, 12, 15, 18, 100, 105, 110, 130]

    column = Column(
        name="numbers",
        data_type="int",
        values=values,
        segment_size=4,
    )

    imprints = ColumnImprints(column, group_size=4, num_bins=8)

    eq_result = imprints.scan(Eq("numbers", 105))
    print("EQ row IDs for 105:", eq_result.row_ids)
    print("EQ metrics:", eq_result.metrics)

    range_result = imprints.scan(Between("numbers", 103, 120))
    print("BETWEEN row IDs for [103, 120]:", range_result.row_ids)
    print("BETWEEN metrics:", range_result.metrics)


if __name__ == "__main__":
    main()