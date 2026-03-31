from __future__ import annotations

from abc import ABC, abstractmethod


class Predicate(ABC):
    """
    Base class for all predicates.

    Every predicate targets one column and must implement
    matches(value) -> bool.
    """

    def __init__(self, column: str):
        self.column = column

    @abstractmethod
    def matches(self, value) -> bool:
        """
        Return True if the value satisfies the predicate.
        """
        raise NotImplementedError


class Eq(Predicate):
    """
    Equality predicate.

    Example:
    status = "A"
    """

    def __init__(self, column: str, value):
        super().__init__(column)
        self.value = value

    def matches(self, value) -> bool:
        return value == self.value


class Between(Predicate):
    """
    Range predicate.

    Example:
    timestamp BETWEEN 100 AND 500
    """

    def __init__(self, column: str, low, high):
        super().__init__(column)
        self.low = low
        self.high = high

    def matches(self, value) -> bool:
        return self.low <= value <= self.high