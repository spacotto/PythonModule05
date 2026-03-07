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
        self.batch = []

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self.batch = data_batch

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        if criteria is None:
            return data_batch
        return [data for data in data_batch if criteria in data]

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
        valid_readings = []

        try:
            for item in data_batch:
                if ":" not in item:
                    raise ValueError(f"Invalid format: {item}")
                sensor, reading = item.split(":", 1)
                if sensor not in ["temperature", "humidity", "pressure"]:
                    raise ValueError(f"Invalid sensor {sensor}")
                try:
                    float(reading)
                except ValueError:
                    int(reading)
                valid_readings.append(item)
            reading_list = ", ".join(valid_readings)
            return f" {bold('Processing sensor batch:')} [{reading_list}]"

        except (ValueError, TypeError, AttributeError) as e:
            return f" Error: Invalid element found in batch. {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        filtered = super().filter_data(data_batch, criteria)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""

        print(f" {bold('Sensor analysis:')} {n} readings processed, avg temp: {avg}")

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
        validated_transactions = []
        
        try:
            for item in data_batch:
                if ":" not in item:
                    raise ValueError(f"Invalid format: {item}")
                action, value_str = item.split(":", 1)
                if action not in ["buy", "sell"]:
                    raise ValueError(f"Invalid action: {action}")
                int(value_str)
                validated_transactions.append(item)
            trans_list = ", ".join(validated_transactions)
            return f" {bold('Processing transaction batch:')} [{trans_list}]"
        
        except (ValueError, TypeError, AttributeError) as e:
            return f" Error: Invalid element found in batch. {e}"
            
    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        filtered = super().filter_data(data_batch, criteria)
        return filtered
            
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
        valid_events = []

        try:
            for event in data_batch:
                if not isinstance(event, str):
                    raise TypeError(f"Events must be str.")
                if event not in ["error", "login", "logout"]:
                    raise ValueError(f"Invalid event: {event}")
                valid_events.append(event)
            events_str = ", ".join(valid_events)
            return f" {bold('Processing event batch:')} [{events_str}]"
        
        except (TypeError, ValueError) as e:
            return f" Error: Invalid element found in batch. {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        filtered = super().filter_data(data_batch, criteria)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        pass


class StreamProcessor:
    """Manages and processes multiple stream types
    through a unified polymorphic interface."""
    def process_stream(self, stream: DataStream, batch: List[Any]) -> str:
        return stream.process_batch(batch)
