from __future__ import annotations

import random
from dataclasses import dataclass

from src.storage.column import Column
from src.storage.table import Table


@dataclass(frozen=True)
class DatasetConfig:
    num_rows: int = 1000
    segment_size: int = 128
    seed: int = 42


class SyntheticDataGenerator:
    def __init__(self, config: DatasetConfig) -> None:
        self.config = config
        self.rng = random.Random(config.seed)

    def build_demo_table(self) -> Table:
        n = self.config.num_rows
        status_values = [self.rng.choice(["ACTIVE", "PENDING", "SUSPENDED", "CLOSED"]) for _ in range(n)]
        clustered_ids = sorted(self.rng.randint(1, 10_000) for _ in range(n))
        amounts = [self.rng.randint(1, 1000) for _ in range(n)]

        columns = {
            "status": Column("status", "string", status_values, self.config.segment_size),
            "customer_id": Column("customer_id", "int", clustered_ids, self.config.segment_size),
            "amount": Column("amount", "int", amounts, self.config.segment_size),
        }
        return Table(name="demo_sales", columns=columns)
