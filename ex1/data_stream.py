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
        self._stream_id = self._format_id(stream_id)
        self._batch = []

    def _format_id(self, stream_id: str) -> str:
        """Validate and format stream ID to 3-digit string."""
        try:
            num = int(stream_id)
            if num < 1 or num > 100:
                raise ValueError("Stream ID must be between 1 and 100")
            return f"{num:03d}"
        except ValueError as e:
            print(f" {e}")
            return None

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._batch = data_batch

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        if criteria is None:
            return data_batch
        return [data for data in data_batch if criteria in data]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        return {"stream_id": self._stream_id}


class SensorStream(DataStream):
    """Handles environmental sensor data streams
    (temperature, humidity, pressure)."""

    def __init__(self, stream_id: str) -> None:
        """Print header and Stream ID format."""
        print(bold(" Initializing Sensor Stream..."))
        super().__init__(stream_id)
        if self._stream_id:
            print(f" {bold('Stream ID:')} SENSOR_{self._stream_id}, Type: Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._batch = []

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
                self._batch.append(item)
            batch_str = ", ".join(self._batch)
            print(f" {bold('Processing sensor batch:')} [{batch_str}]")
            return "OK"

        except (ValueError, TypeError, AttributeError) as e:
            print(f" Error: Invalid element found in batch. {e}")
            return "KO"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        filtered = super().filter_data(data_batch, criteria)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        stats: dict = {}

        stream_id: str = ""
        readings_processed: int = 0
        avg_t: float = 0.0
        avg_h: float = 0.0
        avg_p: float = 0.0
        csa: int = 0

        stream_id = self._stream_id if self._stream_id else "Invalid Stream ID"

        if self._batch:
            readings_processed = len(self._batch)

        temps = [float(item.split(":")[1]) for item in self._batch if "temperature" in item]
        avg_t = sum(temps) / len(temps) if temps else 0.0

        hums = [float(item.split(":")[1]) for item in self._batch if "humidity" in item]
        avg_h = sum(hums) / len(hums) if hums else 0.0

        press = [float(item.split(":")[1]) for item in self._batch if "pressure" in item]
        avg_p = sum(press) / len(press) if press else 0.0

        thresholds = {"temperature": 800, "humidity": 850, "pressure": 900}
        if self._batch:
            csa = len([item for item in self._batch if float(item.split(":")[1]) > threshold])

        stats = {"stream_id": stream_id,
                 "readings_processed": readings_processed,
                 "avg_temperature": avg_t,
                 "avg_humidity": avg_h,
                 "avg_pressure": avg_p,
                 "critical_sensor_alerts": csa}

        return stats

class TransactionStream(DataStream):
    """Handles financial transaction data streams (buy/sell operations)."""

    def __init__(self, stream_id: str) -> None:
        """Print header and Stream ID format."""
        print(bold(" Initializing Transaction Stream..."))
        super().__init__(stream_id)
        if self._stream_id:
            print(f" {bold('Stream ID:')} TRANS_{self._stream_id}, Type: Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._batch = []
        
        try:
            for item in data_batch:
                if ":" not in item:
                    raise ValueError(f"Invalid format: {item}")
                action, value_str = item.split(":", 1)
                if action not in ["buy", "sell"]:
                    raise ValueError(f"Invalid action: {action}")
                int(value_str)
                self._batch.append(item)
            batch_str = ", ".join(self._batch)
            print(f" {bold('Processing transaction batch:')} [{batch_str}]")
            return "OK"
        
        except (ValueError, TypeError, AttributeError) as e:
            print(f" Error: Invalid element found in batch. {e}")
            return "KO"
            
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
        print(bold(" Initializing Event Stream..."))
        super().__init__(stream_id)
        if self._stream_id:
            print(f" {bold('Stream ID:')} EVENT_{self._stream_id}, Type: System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._batch = []

        try:
            for event in data_batch:
                if not isinstance(event, str):
                    raise TypeError(f"Events must be str.")
                if event not in ["error", "login", "logout"]:
                    raise ValueError(f"Invalid event: {event}")
                self._batch.append(event)
            batch_str = ", ".join(self._batch)
            print(f" {bold('Processing event batch:')} [{batch_str}]")
            return "OK"

        except (TypeError, ValueError) as e:
            print(f" Error: Invalid element found in batch. {e}")
            return "KO"

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
