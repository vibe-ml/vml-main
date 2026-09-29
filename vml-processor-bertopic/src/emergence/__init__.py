"""Emergence stage: score publication share for discovery topics in a topic run."""

from src.emergence.models import EmergenceError, EmergenceResult
from src.emergence.observe import observe_emergence

__all__ = [
    "EmergenceError",
    "EmergenceResult",
    "observe_emergence",
]
