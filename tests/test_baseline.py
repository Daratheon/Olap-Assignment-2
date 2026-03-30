from src.baseline.scan import BaselineScanner
from src.query.predicates import Between, Eq
from src.storage.column import Column


def test_equality_scan_returns_correct_row_ids() -> None:
    column = Column(name="x", data_type="int", values=[5, 7, 7, 9, 12, 15, 15, 20], segment_size=4)
    scanner = BaselineScanner()

    result = scanner.scan_column(column, Eq(column="x", target=7))

    assert result.row_ids == [1, 2]
    assert result.metrics.values_scanned == 8
    assert result.metrics.segments_scanned == 2


def test_range_scan_returns_correct_row_ids() -> None:
    column = Column(name="x", data_type="int", values=[5, 7, 7, 9, 12, 15, 15, 20], segment_size=4)
    scanner = BaselineScanner()

    result = scanner.scan_column(column, Between(column="x", lower=8, upper=16))

    assert result.row_ids == [3, 4, 5, 6]
