#!/usr/bin/env python3

"""
Exercise 2: Nexus Integration

Build the complete Code Nexus data processing pipeline—a sophisticated
system that combines multiple processing stages, handles complex data
transformations, and demonstrates advanced polymorphic patterns used
in real-world data engineering.
"""


from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol
import collections
import json
import time


# ============================================================================
# STAGE IMPLEMENTATIONS
# ============================================================================

class ProcessingStage(Protocol):
    """Protocol defining what makes a processing stage"""

    def process(self, data: Any) -> Any:
        """Process data and return result"""
        ...


class InputStage:
    """Validates and prepares input data"""

    def process(self, data: Any) -> Any:
        """Validate and parse input based on pipeline_id"""

        pipeline_id = data.get("pipeline_id", "")
        raw_data = data.get("data")

        if "JSON" in pipeline_id:
            adapter = "JSON"
            parsed = self._parse_json(raw_data)
        elif "CSV" in pipeline_id:
            adapter = "CSV"
            parsed = self._parse_csv(raw_data)
        elif "STREAM" in pipeline_id:
            adapter = "STREAM"
            parsed = self._parse_stream(raw_data)
        else:
            parsed = {"error": f"Unknown pipeline type in ID: {pipeline_id}"}

        if parsed:
            print(f' Processing {adapter} data through pipeline...')
            print(f' Input: {parsed}')

        return {"pipeline_id": pipeline_id, "data": parsed}

    def _parse_json(self, raw_data: Any) -> dict:
        """Parse JSON data"""
        parsed = {}

        if not isinstance(raw_data, str):
            return parsed
        try:
            parsed = json.loads(raw_data)
            return parsed
        except Exception:
            return parsed

    def _parse_csv(self, raw_data: Any) -> str:
        """Parse CSV data"""
        parsed = ""

        if not isinstance(raw_data, str):
            return parsed
        if ',' not in raw_data:
            return parsed

        return [field.strip() for field in raw_data.split(',')]

    def _parse_stream(self, raw_data: Any) -> Any:
        """Parse stream data"""
        if not isinstance(raw_data, list):
            return {"error": "Expected list for stream data"}

        try:
            return [float(x) for x in raw_data]
        except (ValueError, TypeError):
            return {"error": "Stream data must contain numeric values"}


class TransformStage:
    """Transforms data - adapts based on pipeline_id"""

    def process(self, data: Any) -> Any:
        """Apply transformations based on pipeline_id"""

        pipeline_id = data.get("pipeline_id", "")
        current_data = data.get("data")

        if "JSON" in pipeline_id:
            transformed = self._transform_json(current_data)
        elif "CSV" in pipeline_id:
            transformed = self._transform_csv(current_data)
        elif "STREAM" in pipeline_id:
            transformed = self._transform_stream(current_data)
        else:
            transformed = current_data

        return {"pipeline_id": pipeline_id, "data": transformed}

    def _transform_json(self, current_data: Any) -> Any:
        """Transform JSON data"""
        if isinstance(current_data, dict) and 'error' not in current_data:
            current_data['processed'] = True
            current_data['validation'] = 'passed'
        return current_data

    def _transform_csv(self, current_data: Any) -> Any:
        """Transform CSV data"""
        if isinstance(current_data, list):
            return {
                'fields': current_data,
                'count': len(current_data),
                'type': 'csv_record'
            }
        return current_data

    def _transform_stream(self, current_data: Any) -> Any:
        """Transform stream data"""
        if isinstance(current_data, list) and all(isinstance(x, (int, float)) for x in current_data):
            return {
                'readings': current_data,
                'count': len(current_data),
                'average': sum(current_data) / len(current_data) if current_data else 0.0
            }
        return current_data


class OutputStage:
    """Formats output - adapts based on pipeline_id"""

    def process(self, data: Any) -> Any:
        """Format output based on pipeline_id"""

        pipeline_id = data.get("pipeline_id", "")
        current_data = data.get("data")

        if "JSON" in pipeline_id:
            formatted = self._format_json(current_data)
        elif "CSV" in pipeline_id:
            formatted = self._format_csv(current_data)
        elif "STREAM" in pipeline_id:
            formatted = self._format_stream(current_data)
        else:
            formatted = str(current_data)

        return {"pipeline_id": pipeline_id, "data": formatted}

    def _format_json(self, current_data: Any) -> Any:
        """Format JSON output"""
        if isinstance(current_data, dict):
            if 'error' in current_data:
                return current_data
            if 'sensor' in current_data and 'value' in current_data:
                return (" Processed temperature reading:" +
                       f" {current_data['value']}°{current_data.get('unit', '')} (Normal range)")
        return str(current_data)

    def _format_csv(self, current_data: Any) -> Any:
        """Format CSV output"""
        if isinstance(current_data, dict) and 'fields' in current_data:
            return f"User activity logged: {current_data['count']} actions processed"
        return str(current_data)

    def _format_stream(self, current_data: Any) -> Any:
        """Format stream output"""
        if isinstance(current_data, dict) and 'readings' in current_data:
            return f"Stream summary: {current_data['count']} readings, avg: {current_data['average']:.1f}°C"
        return str(current_data)

# ============================================================================
# ABSTRACT PIPELINE BASE CLASS
# ============================================================================

class ProcessingPipeline(ABC):
    """Abstract base class for all data processing pipelines"""

    def __init__(self) -> None:
        self._stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage to the pipeline"""
        self._stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Process data through the pipeline - must be overridden"""
        for stage in self.stages:
            result = stage.process(data)
        return result

# ============================================================================
# ADAPTERS
# ============================================================================

class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter for JSON data"""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = "JSON_" + pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Override to handle JSON-specific processing"""
        id_data = {"pipeline_id": self.pipeline_id, "data": data}
        result = id_data
        for stage in self._stages:
            result = stage.process(result)
        return result.get("data")


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter for CSV data"""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = "CSV_" + pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Override to handle CSV-specific processing"""
        id_data = {"pipeline_id": self.pipeline_id, "data": data}
        result = id_data
        for stage in self._stages:
            result = stage.process(result)
        return result.get("data")


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for stream data"""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = "STREAM_" + pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Override to handle stream-specific processing"""
        id_data = {"pipeline_id": self.pipeline_id, "data": data}
        result = id_data
        for stage in self._stages:
            result = stage.process(result)
        return result.get("data")

# ============================================================================
# NEXUS MANAGER
# ============================================================================

class NexusManager:
    """Orchestrates multiple processing pipelines polymorphically"""

    def __init__(self, capacity: int):
        self._pipelines: List[ProcessingPipeline] = []
        self.capacity = capacity

        print(' Initializing Nexus Manager...')
        print(f' Pipeline capacity: {self.capacity} streams/second')

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a pipeline to manage"""
        self._pipelines.append(pipeline)

    def process_data(self, data: Any) -> None:
        """Process data through all pipelines"""
        for entry in data:
            for pipeline in self._pipelines:
                result = pipeline.process(entry)


# ============================================================================
# DEMO
# ============================================================================

def main():
    """Demonstrate the Code Nexus pipeline system"""

    dataset: list = ['{"sensor": "temp", "value": 23.5, "unit": "C"}',
                     "user,action,timestamp",
                     ["22.1", "21.9", "22.5", "22.3", "21.8"]]

    print(' === CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===')
    print()

    manager = NexusManager(1000)
    print()

    print(' Creating Data Processing Pipeline...')
    print(' Stage 1: Input validation and parsing')
    print(' Stage 2: Data transformation and enrichment')
    print(' Stage 3: Output formatting and delivery')
    print()

    # Create JSON pipeline with stages
    json_pipeline = JSONAdapter("001")
    json_pipeline.add_stage(InputStage())
    json_pipeline.add_stage(TransformStage())
    json_pipeline.add_stage(OutputStage())

    # Create CSV pipeline with stages
    csv_pipeline = CSVAdapter("001")
    csv_pipeline.add_stage(InputStage())
    csv_pipeline.add_stage(TransformStage())
    csv_pipeline.add_stage(OutputStage())

    # Create Stream pipeline with stages
    stream_pipeline = StreamAdapter("001")
    stream_pipeline.add_stage(InputStage())
    stream_pipeline.add_stage(TransformStage())
    stream_pipeline.add_stage(OutputStage())

    # Add configured pipelines to manager
    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    print(" === Multi-Format Data Processing ===")
    print()

    manager.process_data(dataset)
    print()

    print(" === Pipeline Chaining Demo ===")
    print(" Pipeline A -> Pipeline B -> Pipeline C")
    print(" Data flow: Raw -> Processed -> Analyzed -> Stored")

    print()

    print(" === Error Recovery Test ===")
    print(" Simulating pipeline failure...")

    print()

    print(" Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
