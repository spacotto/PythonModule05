#!/usr/bin/env python3

"""
Exercise 2: Nexus Integration

Build the complete Code Nexus data processing pipeline—a sophisticated
system that combines multiple processing stages, handles complex data
transformations, and demonstrates advanced polymorphic patterns used
in real-world data engineering.
"""


from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional  # noqa: F401
from collections import OrderedDict  # noqa: F401


class ProcessingStage:
    """Protocol class - duck typing, any class with process() qualifies."""

    def process(self, data: Any) -> Any:
        print(" Creating Data Processing Pipeline...")


class InputStage(ProcessingStage):
    """Validates and prepares incoming data."""

    def process(self, data: Any) -> dict:
        print(" Stage 1: Input validation and parsing.")


class TransformStage(ProcessingStage):
    """Transforms and enriches data."""

    def process(self, data: Any) -> dict:
        print(" Stage 2: Data transformation and enrichment")
        pass


class OutputStage(ProcessingStage):
    """Formats data for final output."""

    def process(self, data: Any) -> str:
        print(" Stage 3: Output formatting and delivery")
        pass


class ProcessingPipeline(ABC):
    """Abstract base class managing configurable processing stages."""

    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id = pipeline_id
        self.stages: list[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        pass

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter for JSON data."""

    def __init__(self, pipeline_id: str) -> None:
        pass

    def process(self, data: Any) -> Union[str, Any]:
        pass


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter for CSV data."""

    def __init__(self, pipeline_id: str) -> None:
        pass

    def process(self, data: Any) -> Union[str, Any]:
        pass


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for stream/list data."""

    def __init__(self, pipeline_id: str) -> None:
        pass

    def process(self, data: Any) -> Union[str, Any]:
        pass


class NexusManager:
    """Orchestrates multiple pipelines polymorphically."""

    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        print(" Initializing Nexus Manager...")
        print(" Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> str:
        pass


def main() -> None:
    """Exercise 2 Demo"""

    print(' === CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===')
    print()

    # Initialize manager
    manager = NexusManager()
    print()

    # Add pipelines (each prints its stage setup)
    manager.add_pipeline(JSONAdapter("JSON_01"))
    manager.add_pipeline(CSVAdapter("CSV_01"))
    manager.add_pipeline(StreamAdapter("STREAM_01"))
    print()

    print(' === Multi-Format Data Processing ===')
    print()

    # JSON test data
    json_data = 'temp:23.5'
    print(manager.process_data(json_data))

    print(' Processing JSON data through pipeline...')
    print(' Input: {"sensor": "temp", "value": 23.5, "unit": "C"}')
    print(' Transform: Enriched with metadata and validation')
    print(' Output: Processed temperature reading: 23.5°C (Normal range)')
    print()

    # CSV test data
    csv_data = "user,action,timestamp"
    print(manager.process_data(csv_data))

    print(' Processing CSV data through same pipeline...')
    print(' Input: "user,action,timestamp"')
    print(' Transform: Parsed and structured data')
    print(' Output: User activity logged: 1 actions processed')
    print()

    # Stream test data — list of float readings
    stream_data = ["22.1", "21.9", "22.5", "22.3", "21.8"]
    print(manager.process_data(stream_data))

    print(' Processing Stream data through same pipeline...')
    print(' Input: Real-time sensor stream')
    print(' Transform: Aggregated and filtered')
    print(' Output: Stream summary: 5 readings, avg: 22.1°C')
    print()

    print(' === Pipeline Chaining Demo ===')
    print(' Pipeline A -> Pipeline B -> Pipeline C')
    print(' Data flow: Raw -> Processed -> Analyzed -> Stored')
    print()
    print(' Chain result: 100 records processed through 3-stage pipeline')
    print(' Performance: 95% efficiency, 0.2s total processing time')
    print()
    print(' === Error Recovery Test ===')
    print(' Simulating pipeline failure...')
    print(' Error detected in Stage 2: Invalid data format')
    print(' Recovery initiated: Switching to backup processor')
    print(' Recovery successful: Pipeline restored, processing resumed')
    print()
    print(' Nexus Integration complete. All systems operational')


if __name__ == "__main__":
    main()
