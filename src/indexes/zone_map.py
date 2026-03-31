from __future__ import annotations

from dataclasses import dataclass

from src.baseline.scan import ScanResult
from src.metrics.metrics import QueryMetrics
from src.query.predicates import Between, Eq, Predicate
from src.storage.column import Column


@dataclass(frozen=True)
class SegmentZoneMap:
    """
    Min/max metadata for a single segment.
    """

    segment_id: int
    start_row_id: int
    min_value: object
    max_value: object


class ZoneMapIndex:
    """
    Builds and stores per-segment min/max metadata for a column.
    """

    def __init__(self, column: Column) -> None:
        self.column = column
        self.segment_maps: list[SegmentZoneMap] = self._build()

    def _build(self) -> list[SegmentZoneMap]:
        segment_maps: list[SegmentZoneMap] = []

        for segment in self.column.iter_segments():
            if not segment.values:
                continue

            segment_maps.append(
                SegmentZoneMap(
                    segment_id=segment.segment_id,
                    start_row_id=segment.start_row_id,
                    min_value=min(segment.values),
                    max_value=max(segment.values),
                )
            )

        return segment_maps

    def should_scan_segment(self, predicate: Predicate, segment_map: SegmentZoneMap) -> bool:
        """
        Returns True if the segment might contain matches.
        Returns False if the segment can be skipped safely.

        Zone maps are exact for Eq and Between here:
        - no false positives from the metadata rule itself
        - no false negatives
        """

        if isinstance(predicate, Eq):
            value = predicate.value
            return segment_map.min_value <= value <= segment_map.max_value

        if isinstance(predicate, Between):
            low = predicate.low
            high = predicate.high

            # Overlap test between:
            # [segment_min, segment_max] and [low, high]
            return not (segment_map.max_value < low or segment_map.min_value > high)

        # Fallback:
        # if we do not know how to reason about the predicate with min/max,
        # scan the segment to preserve correctness.
        return True


class ZoneMapScanner:
    """
    Uses a zone map to skip segments before scanning values.
    """

    def __init__(self) -> None:
        self._index_cache: dict[tuple[str, str], ZoneMapIndex] = {}

    def _cache_key(self, column: Column) -> tuple[str, str]:
        """
        Cache by (column name, object identity) so repeated queries do not rebuild
        the same zone map during one program run.
        """
        return (column.name, str(id(column)))

    def _get_or_build_index(self, column: Column) -> ZoneMapIndex:
        key = self._cache_key(column)

        if key not in self._index_cache:
            self._index_cache[key] = ZoneMapIndex(column)

        return self._index_cache[key]

    def scan_column(self, column: Column, predicate: Predicate) -> ScanResult:
        matched_row_ids: list[int] = []
        values_scanned = 0
        segments_scanned = 0
        segments_skipped = 0

        zone_map_index = self._get_or_build_index(column)

        for segment, segment_map in zip(column.iter_segments(), zone_map_index.segment_maps):
            if not zone_map_index.should_scan_segment(predicate, segment_map):
                segments_skipped += 1
                continue

            segments_scanned += 1

            for offset, value in enumerate(segment.values):
                values_scanned += 1

                if predicate.matches(value):
                    matched_row_ids.append(segment.start_row_id + offset)

        metrics = QueryMetrics(
            values_scanned=values_scanned,
            segments_scanned=segments_scanned,
            segments_skipped=segments_skipped,
            false_positives=0,
            bytes_used=self._estimate_index_size(zone_map_index),
        )

        return ScanResult(
            row_ids=matched_row_ids,
            metrics=metrics,
        )

    def _estimate_index_size(self, zone_map_index: ZoneMapIndex) -> int:
        """
        Rough educational estimate of bytes used by the zone map metadata.

        We count:
        - min
        - max
        - segment_id
        - start_row_id

        For simplicity, assume 8 bytes each.
        """
        bytes_per_segment_map = 8 * 4
        return len(zone_map_index.segment_maps) * bytes_per_segment_map