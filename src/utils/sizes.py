from __future__ import annotations

import sys
from typing import Any


def approx_bytes_used(obj: Any) -> int:
    """Shallow estimate for quick comparisons in early milestones."""
    return sys.getsizeof(obj)
