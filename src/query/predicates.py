from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Predicate:
    column: str

    def matches(self, value: Any) -> bool:
        raise NotImplementedError


@dataclass(frozen=True)
class Eq(Predicate):
    target: Any

    def matches(self, value: Any) -> bool:
        return value == self.target


@dataclass(frozen=True)
class Between(Predicate):
    lower: Any
    upper: Any
    inclusive_lower: bool = True
    inclusive_upper: bool = True

    def matches(self, value: Any) -> bool:
        lower_ok = value >= self.lower if self.inclusive_lower else value > self.lower
        upper_ok = value <= self.upper if self.inclusive_upper else value < self.upper
        return lower_ok and upper_ok
