"""
Exercise 1: Polymorphic Streams

Create a sophisticated data streaming system that
demonstrates advanced polymorphic behavior.
Build stream handlers that can process mixed data
types while maintaining type-specific optimizations.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional  # noqa: F401


class DataStream(ABC):
    """An abstract base class with core streaming functionality."""

    def __init__(self, stream_id: str) -> None:
        """..."""
        pass

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]
        """Filter data based on criteria."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]
        """Return stream statistics."""
        pass


class SensorStream(DataStream):
    """..."""
    pass


class TransactionStream(DataStream):
    """..."""
    pass


class EventStream(DataStream):
    """..."""
    pass

