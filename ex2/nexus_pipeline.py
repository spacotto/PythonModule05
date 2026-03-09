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


def bold(text: str) -> str:
    """A function making strings of text bold."""
    w, r = "\033[1;97m", "\033[0m"
    return f"{w}{text}{r}"


class ProcessingStage:
    """Protocol class - duck typing, any class with process() qualifies."""

    def process(self, data: Any) -> Any:
        """..."""
        print(" Creating Data Processing Pipeline...")
        print(" Stage 1: Input validation and parsing")
        print(" Stage 2: Data transformation and enrichment")
        print(" Stage 3: Output formatting and delivery")


class InputStage(ProcessingStage):
    """Validates and prepares incoming data."""

    def process(self, data: Any) -> dict:
        """..."""
        pass


class TransformStage(ProcessingStage):
    """Transforms and enriches data."""

    def process(self, data: Any) -> dict:
        """..."""
        pass


class OutputStage(ProcessingStage):
    """Formats data for final output."""

    def process(self, data: Any) -> str:
        """..."""
        pass


class ProcessingPipeline(ABC):
    """An abstract base class with configurable stages."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize pipeline with ID and empty stage list."""
        print(" Creating Data Processing Pipeline...")
        self.pipeline_id = pipeline_id
        self.stages: list[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage to the pipeline."""
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Process data through all stages (must be implemented by subclasses)."""
        pass


class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter for JSON data."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)  # Must call parent
        print(' Processing JSON data through pipeline...')

    def process(self, data: Any) -> Union[str, Any]:  # Must implement!
        """Process JSON data through stages."""
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter for CSV data."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        print(' Processing CSV data through same pipeline...')

    def process(self, data: Any) -> Union[str, Any]:
        """Process CSV data through stages."""
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for stream/list data."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        print(' Processing Stream data through same pipeline...')

    def process(self, data: Any) -> Union[str, Any]:
        """Process Stream data through stages."""
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result


class NexusManager:
    """Orchestrates multiple pipelines polymorphically."""

    def __init__(self) -> None:
        """..."""
        self.pipelines: List[ProcessingPipeline] = []
        print(" Initializing Nexus Manager...")
        print(" Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """..."""
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> str:
        """..."""
        pass


def main() -> None:
    """Exercise 2 Demo"""

    # Data sets
    json_data: str = '{"sensor": "temp", "value": 23.5, "unit": "C"}'
    csv_data: str = "user,action,timestamp"
    stream_data: list = ["22.1", "21.9", "22.5", "22.3", "21.8"]

    print(' === CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===')
    print()

    manager = NexusManager()
    print()

    print()

    print(' === Multi-Format Data Processing ===')
    print()

    print(' === Pipeline Chaining Demo ===')
    print(' Pipeline A -> Pipeline B -> Pipeline C')
    print(' Data flow: Raw -> Processed -> Analyzed -> Stored')
    print()

    print(' === Error Recovery Test ===')
    print()

    print(' Nexus Integration complete. All systems operational')


if __name__ == "__main__":
    main()
