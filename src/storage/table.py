from __future__ import annotations

from dataclasses import dataclass

from .column import Column


@dataclass
class Table:
    name: str
    columns: dict[str, Column]

    def __post_init__(self) -> None:
        lengths = {column.num_rows for column in self.columns.values()}
        if len(lengths) > 1:
            raise ValueError("All columns must have the same row count")

    @property
    def num_rows(self) -> int:
        if not self.columns:
            return 0
        return next(iter(self.columns.values())).num_rows

    def get_column(self, column_name: str) -> Column:
        try:
            return self.columns[column_name]
        except KeyError as exc:
            raise KeyError(f"Unknown column: {column_name}") from exc
