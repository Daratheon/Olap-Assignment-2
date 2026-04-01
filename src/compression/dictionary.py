from __future__ import annotations

from dataclasses import dataclass

from src.metrics.metrics import QueryMetrics
from src.storage.column import Column


@dataclass
class DictionaryQueryResult:
    row_ids: list[int]
    metrics: QueryMetrics


class DictionaryEncodedColumn:
    """
    Dictionary-encoded representation of a column.

    Stores:
    - a dictionary mapping original values -> integer codes
    - a reverse dictionary mapping codes -> original values
    - an encoded list of integer codes for each row

    Equality queries can be answered directly on the encoded values
    without decoding the entire column.
    """

    def __init__(self, column: Column) -> None:
        self.column = column
        self.value_to_code: dict[object, int] = {}
        self.code_to_value: dict[int, object] = {}
        self.encoded_values: list[int] = []

        self._build()

    def _build(self) -> None:
        """
        Build dictionary encoding for the column.

        Example:
            values = ["A", "B", "A", "C"]

        dictionary:
            "A" -> 0
            "B" -> 1
            "C" -> 2

        encoded_values:
            [0, 1, 0, 2]
        """
        next_code = 0

        for value in self.column.values:
            if value not in self.value_to_code:
                self.value_to_code[value] = next_code
                self.code_to_value[next_code] = value
                next_code += 1

            self.encoded_values.append(self.value_to_code[value])

    def lookup_eq(self, target_value) -> DictionaryQueryResult:
        """
        Equality lookup using dictionary codes only.

        If the target value is not in the dictionary, return no matches.
        """
        if target_value not in self.value_to_code:
            return DictionaryQueryResult(
                row_ids=[],
                metrics=QueryMetrics(
                    values_scanned=0,
                    segments_scanned=0,
                    segments_skipped=0,
                    false_positives=0,
                    bytes_used=self.estimate_bytes_used(),
                ),
            )

        target_code = self.value_to_code[target_value]

        row_ids: list[int] = []
        values_scanned = 0

        for row_id, code in enumerate(self.encoded_values):
            values_scanned += 1
            if code == target_code:
                row_ids.append(row_id)

        return DictionaryQueryResult(
            row_ids=row_ids,
            metrics=QueryMetrics(
                values_scanned=values_scanned,
                segments_scanned=0,
                segments_skipped=0,
                false_positives=0,
                bytes_used=self.estimate_bytes_used(),
            ),
        )

    def decode_all(self) -> list[object]:
        """
        Decode the full column back to original values.
        Useful for correctness checks, not required for equality lookup.
        """
        return [self.code_to_value[code] for code in self.encoded_values]

    def estimate_bytes_used(self) -> int:
        """
        Simple educational storage estimate.

        Approximate:
        - 8 bytes per dictionary entry key reference
        - 8 bytes per dictionary entry code
        - 4 bytes per encoded integer code

        This is a rough estimate, which is fine for the assignment.
        """
        dictionary_bytes = len(self.value_to_code) * 16
        encoded_bytes = len(self.encoded_values) * 4
        return dictionary_bytes + encoded_bytes