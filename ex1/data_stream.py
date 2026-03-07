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
        keywords = criteria.split()
        return [data for data in data_batch if any(k in data for k in keywords)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        return {"stream_id": self._stream_id}


class SensorStream(DataStream):
    """Handles environmental sensor data streams
    (temperature, humidity, pressure)."""

    def __init__(self, stream_id: str) -> None:
        """Print header and Stream ID format."""
        self._stream_id: str = ""
        self._readings_processed: int = 0
        self._avg_temperature: float = 0.0
        self._avg_humidity: float = 0.0
        self._avg_pressure: float = 0.0
        self._critical_sensor_alerts: int = 0
        
        super().__init__(stream_id)
        if self._stream_id:
            self._stream_id = "SENSOR_" + self._stream_id

    def _parse_batch(self, data_batch: List[Any]) -> None:
        """Parse and validate batch, store valid items in self._batch."""
        self._batch = []

        for item in data_batch:
        
            try:
                if ":" not in item:
                    raise ValueError(f"Invalid format: {item}")
                sensor, reading = item.split(":", 1)
                if sensor not in ["temperature", "humidity", "pressure"]:
                    raise ValueError(f"Invalid sensor: {sensor}")
                try:
                    float(reading)
                except ValueError:
                    int(reading)
                self._batch.append(item)

            except (ValueError, TypeError, AttributeError):
                continue

    def _run_analysis(self) -> None:
        """Compute and store stats from self._batch."""
        thresholds = {"temperature": 40, "humidity": 80, "pressure": 1040}
        self._readings_processed = len(self._batch)
        temps = [float(i.split(":")[1]) for i in self._batch if "temperature" in i]
        self._avg_temperature = sum(temps) / len(temps) if temps else 0.0
        hums = [float(i.split(":")[1]) for i in self._batch if "humidity" in i]
        self._avg_humidity = sum(hums) / len(hums) if hums else 0.0
        press = [float(i.split(":")[1]) for i in self._batch if "pressure" in i]
        self._avg_pressure = sum(press) / len(press) if press else 0.0
        self._critical_sensor_alerts = len([i for i in self._batch
                                            if float(i.split(":")[1]) >
                                            thresholds[i.split(":")[0]]])

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._parse_batch(data_batch)
        if not self._batch:
            print(f" Error: No valid sensor data found in batch.")
            return ""
        self._run_analysis()
        batch_str = ", ".join(self._batch)
        return batch_str

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        filtered = super().filter_data(data_batch, criteria)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        stats: dict = {}

        stats = {"stream_id": self._stream_id,
                 "readings_processed": self._readings_processed,
                 "avg_temperature": self._avg_temperature,
                 "avg_humidity": self._avg_humidity,
                 "avg_pressure": self._avg_pressure,
                 "critical_sensor_alerts": self._critical_sensor_alerts}

        return stats

class TransactionStream(DataStream):
    """Handles financial transaction data streams (buy/sell operations)."""

class TransactionStream(DataStream):
    """Handles financial transaction data streams (buy/sell operations)."""

    def __init__(self, stream_id: str) -> None:
        """Print header and Stream ID format."""
        self._stream_id: str = ""
        self._operations: int = 0
        self._net_flow: str = ""
        self._large_transactions: int = 0
        
        super().__init__(stream_id)
        if self._stream_id:
            self._stream_id = "TRANS_" + self._stream_id

    def _parse_batch(self, data_batch: List[Any]) -> None:
        """Parse and validate batch, store valid items in self._batch."""
        self._batch = []

        for item in data_batch:
            try:
                if ":" not in item:
                    raise ValueError(f"Invalid format: {item}")
                action, value_str = item.split(":", 1)
                if action not in ["buy", "sell"]:
                    raise ValueError(f"Invalid action: {action}")
                int(value_str)
                self._batch.append(item)
            except (ValueError, TypeError, AttributeError):
                continue

    def _run_analysis(self) -> None:
        """Compute and store stats from self._batch."""
        self._operations = len(self._batch)

        net = sum(int(i.split(":")[1]) if "buy" in i
                  else -int(i.split(":")[1]) for i in self._batch)
        self._net_flow = f"+{net}" if net >= 0 else f"{net}"

        threshold = 750
        self._large_transactions = len([i for i in self._batch
                                        if int(i.split(":")[1]) > threshold])

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._parse_batch(data_batch)
        if not self._batch:
            print(f" Error: No valid transaction data found in batch.")
            return ""
        self._run_analysis()
        batch_str = ", ".join(self._batch)
        return batch_str

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        stats: dict = {}

        stats = {"stream_id": self._stream_id,
                 "operations": self._operations,
                 "net_flow": self._net_flow,
                 "large_transactions": self._large_transactions}

        return stats


class EventStream(DataStream):
    """Handles system event data streams (login, logout, errors)."""

    def __init__(self, stream_id: str) -> None:
        """Print header and Stream ID format."""
        self._stream_id: str = ""
        self._events: int = 0
        self._errors: int = 0
        
        super().__init__(stream_id)
        if self._stream_id:
            self._stream_id = "EVENT_" + self._stream_id

    def _parse_batch(self, data_batch: List[Any]) -> None:
        """Parse and validate batch, store valid items in self._batch."""
        self._batch = []

        for item in data_batch:
            try:
                if not isinstance(item, str):
                    raise TypeError(f"Events must be str.")
                if item not in ["error", "login", "logout"]:
                    raise ValueError(f"Invalid event: {item}")
                self._batch.append(item)
            except (TypeError, ValueError):
                continue

    def _run_analysis(self) -> None:
        """Compute and store stats from self._batch."""
        self._events = len(self._batch)
        self._errors = len([i for i in self._batch if i == "error"])

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        self._parse_batch(data_batch)
        if not self._batch:
            print(f" Error: No valid event data found in batch.")
            return ""
        self._run_analysis()
        batch_str = ", ".join(self._batch)
        return batch_str

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria."""
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        stats: dict = {}

        stats = {"stream_id": self._stream_id,
                 "events": self._events,
                 "errors": self._errors}

        return stats


class StreamProcessor:
    """Manages and processes multiple stream types
    through a unified polymorphic interface."""

    def __init__(self) -> None:
        self._streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Add a stream to the processor."""
        self._streams.append(stream)

    def process_stream(self, stream: DataStream, batch: List[Any]) -> str:
        """Process a single stream with the given batch."""
        return stream.process_batch(batch)

    def display_analysis(self, data_batch: List[Any]) -> None:
        """Process all streams and print unified report."""
        print(" Processing mixed stream types through unified interface...")
        print()
        print(" Batch 1 Results:")
    
        csa = 0
        large = 0
    
        for stream in self._streams:
            stream.process_batch(data_batch)
            stats = stream.get_stats()
            if isinstance(stream, SensorStream):
                print(f" - Sensor data: {stats['readings_processed']} readings processed")
                csa += stats.get('critical_sensor_alerts', 0)
            elif isinstance(stream, TransactionStream):
                print(f" - Transaction data: {stats['operations']} operations processed")
                large += stats.get('large_transactions', 0)
            elif isinstance(stream, EventStream):
                print(f" - Event data: {stats['events']} events processed")
    
        print()
        print(" Stream filtering active: High-priority data only")
        print(f" Filtered results: {csa} critical sensor alerts, {large} large transaction(s)")
        print()
        print(" All streams processed successfully. Nexus throughput optimal.")


def main() -> None:
    """Exercise 1 Demo"""

    stream_id = "1"
    
    data_batch0 = ["temperature:22.5", "humidity:65", "pressure:1013",
                  "buy:100", "sell:150", "buy:75",
                  "login", "error", "logout"]

    ss = SensorStream(stream_id)
    s1 = ss.process_batch(data_batch0)
    ds = ss.get_stats()

    ts = TransactionStream(stream_id)
    t1 = ts.process_batch(data_batch0)
    dt = ts.get_stats()

    es = EventStream(stream_id)
    e1 = es.process_batch(data_batch0)
    de = es.get_stats()

    print(" === CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()
    print(" Initializing Sensor Stream...")
    print(f" Stream ID: {ds['stream_id']}, Type: Environmental Data")
    print(f" Processing sensor batch: [{s1}]")
    print(f" Sensor analysis: {ds['readings_processed']} " +
          f"readings processed, avg temp: {ds['avg_temperature']}°C")
    print()
    print(" Initializing Transaction Stream...")
    print(f" Stream ID: {dt['stream_id']}, Type: Financial Data")
    print(f" Processing transaction batch: [{t1}]")
    print(f" Transaction analysis: {dt['operations']} operations," +
          f" net flow: {dt['net_flow']} units")
    print()
    print(" Initializing Event Stream...")
    print(f" Stream ID: {de['stream_id']}, Type: System Events")
    print(f" Processing event batch: [{e1}]")
    print(f" Event analysis: {de['events']} events, {de['errors']} error detected")
    print()

    data_batch1 = ["temperature:45", "humidity:85", "buy:800", "sell:100",
                   "buy:400", "buy:200", "login", "error", "logout"]

    sp = StreamProcessor()
    sp.add_stream(ss)
    sp.add_stream(ts)
    sp.add_stream(es)

    print(" === Polymorphic Stream Processing ===")
    sp.display_analysis(data_batch1)


if __name__ == "__main__":
    main()
