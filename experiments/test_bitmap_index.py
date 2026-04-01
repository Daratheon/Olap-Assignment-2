from src.indexes.bitmap_index import BitmapIndex
from src.storage.column import Column


def main():
    status = ["A", "B", "A", "C", "B", "A"]

    column = Column(
        name="status",
        data_type="str",
        values=status,
        segment_size=3,
    )

    bitmap_index = BitmapIndex(column)

    result_a = bitmap_index.lookup_eq("A")
    print("A row IDs:", result_a.row_ids)
    print("A metrics:", result_a.metrics)

    bitmap_a = bitmap_index.get_bitmap("A")
    bitmap_b = bitmap_index.get_bitmap("B")

    print("Bitmap A:", bitmap_a)
    print("Bitmap B:", bitmap_b)
    print("A AND B:", bitmap_index.and_bitmap(bitmap_a, bitmap_b))
    print("A OR B:", bitmap_index.or_bitmap(bitmap_a, bitmap_b))
    print("NOT A:", bitmap_index.not_bitmap(bitmap_a))


if __name__ == "__main__":
    main()