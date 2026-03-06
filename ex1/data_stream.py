"""
Exercise 1: Polymorphic Streams

Create a sophisticated data streaming system that
demonstrates advanced polymorphic behavior.
Build stream handlers that can process mixed data
types while maintaining type-specific optimizations.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional  # noqa: F401


def bold(text: str) -> str:
    """A function making strings of text bold."""
    w, r = "\033[1;97m", "\033[0m"
    return f"{w}{text}{r}"


class DataStream(ABC):
    """An abstract base class with core streaming functionality."""

    def __init__(self, stream_id: str) -> None:
        """Print Stream ID."""
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        pass


class SensorStream(DataStream):
    """Handles environmental sensor data streams
    (temperature, humidity, pressure)."""

    def __init__(self, stream_id: str):
        """Print header and Stream ID format."""
        super().__init__(stream_id)

        print(bold(" Initializing Sensor Stream..."))
        try:
            num = int(self.stream_id)
            if num < 1 or num > 100:
                raise ValueError(f"Stream ID must be between 1 and 100")
            n = f"{num:03d}"
            print(f" {bold('Stream ID:')} SENSOR_{n}, Type: Environmental Data")
        except ValueError as e:
            print(f" {e}")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        pass


class TransactionStream(DataStream):
    """Handles financial transaction data streams (buy/sell operations)."""

    def __init__(self, stream_id: str):
        """Print header and Stream ID format."""
        super().__init__(stream_id)

        print(bold(" Initializing Transaction Stream..."))
        try:
            num = int(self.stream_id)
            if num < 1 or num > 100:
                raise ValueError(f"Stream ID must be between 1 and 100")
            n = f"{num:03d}"
            print(f" {bold('Stream ID:')} TRANS_{n}, Type: Financial Data")
        except ValueError as e:
            print(f" {e}")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        pass


class EventStream(DataStream):
    """Handles system event data streams (login, logout, errors)."""

    def __init__(self, stream_id: str):
        """Print header and Stream ID format."""
        super().__init__(stream_id)

        print(bold(" Initializing Event Stream..."))
        try:
            num = int(self.stream_id)
            if num < 1 or num > 100:
                raise ValueError(f"Stream ID must be between 1 and 100")
            n = f"{num:03d}"
            print(f" {bold('Stream ID:')} EVENT_{n}, Type: System Events")
        except ValueError as e:
            print(f" {e}")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        pass


class StreamProcessor:
    """Manages and processes multiple stream types
    through a unified polymorphic interface."""
    def process_stream(self, stream: DataStream, batch: List[Any]) -> str:
        return stream.process_batch(batch)
