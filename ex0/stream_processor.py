"""
Exercise 0: Data Processor.

Implements a polymorphic data processing system using abstract base classes.
A base DataProcessor class defines a common interface, and three specialized
subclasses (NumericProcessor, TextProcessor, LogProcessor) override its
methods to handle their specific data types while maintaining interface
consistency.
"""


from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional  # noqa: F401


class DataProcessor(ABC):
    """Abstract base class defining the common data processing interface."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data and return a result string."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if the data is appropriate for this processor."""
        pass

    def format_output(self, result: str) -> str:
        """Format the result string for output."""
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """Processor specifically for numerical list data."""

    def __init__(self):
        print("Initializing Numeric Processor...")

    def validate(self, data: Any) -> bool:
        """Checks if the input is a list of integers or floats."""
        if isinstance(data, list) and all(isinstance(x, (int, float)) for x in data):
            print("Validation: Numeric data verified")
            return True
        return False

    def process(self, data: List[Union[int, float]]) -> str:
        """Calculates count, sum, and average from the numeric list."""
        print(f"Processing data: {data}")

        count = len(data)
        total = sum(data)
        avg = total / count if count > 0 else 0

        return (f"Processed {count} numeric values, "
                f"sum={total}, avg={float(avg)}")

    def format_output(self, result: str) -> str:
        """Overrides base method to match the requested output style."""
        return f"Output: {result}"


class TextProcessor(DataProcessor):
    """Processor specifically for string data."""

    def __init__(self):
        print("Initializing Text Processor...")

    def validate(self, data: Any) -> bool:
        """Checks if the input is a non-empty string."""
        if isinstance(data, str) and len(data) > 0:
            print("Validation: Text data verified")
            return True
        return False

    def process(self, data: str) -> str:
        """Calculates character count and word count."""
        print(f'Processing data: "{data}"')

        char_count = len(data)
        word_count = len(data.split())

        return f"Processed text: {char_count} characters, {word_count} words"

    def format_output(self, result: str) -> str:
        """Formats the final output string."""
        return f"Output: {result}"


class LogProcessor(DataProcessor):
    """Processor specifically for log message strings."""

    def __init__(self):
        print("Initializing Log Processor...")

    def validate(self, data: Any) -> bool:
        """Checks if the input is a string containing a colon."""
        if isinstance(data, str) and ":" in data:
            print("Validation: Log entry verified")
            return True
        return False

    def process(self, data: str) -> str:
        """Parses the log level and the message content."""
        print(f'Processing data: "{data}"')

        parts = data.split(":", 1)
        log_level = parts[0].strip()
        message = parts[1].strip()

        return f"{log_level} level detected: {message}"

    def format_output(self, result: str) -> str:
        """Custom formatting to add the ALERT prefix for logs."""
        return f"Output: [ALERT] {result}"
