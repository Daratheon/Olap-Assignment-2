from src.compression.dictionary import DictionaryEncodedColumn
from src.storage.column import Column


def main():
    values = ["A", "B", "A", "C", "B", "A"]

    column = Column(
        name="status",
        data_type="str",
        values=values,
        segment_size=3,
    )

    encoded = DictionaryEncodedColumn(column)

    print("value_to_code:", encoded.value_to_code)
    print("encoded_values:", encoded.encoded_values)

    result = encoded.lookup_eq("A")
    print("Row IDs for A:", result.row_ids)
    print("Metrics:", result.metrics)

    decoded = encoded.decode_all()
    print("Decoded values:", decoded)


if __name__ == "__main__":
    main()