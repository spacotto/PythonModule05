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


def bold(text: str) -> str:
    """A function making strings of text bold."""
    w, r = "\033[1;97m", "\033[0m"
    return f"{w}{text}{r}"


class InvalidLogFormatError(Exception):
    """Raised when log data doesn't match expected format."""
    def __init__(self) -> None:
        self.message = "Invalid log format. Try: ERROR/INFO: Message."
        super().__init__(self.message)


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
        result = "TBA"
        return f" {bold('Output:')} {result}"


class NumericProcessor(DataProcessor):
    """Processor specifically for numerical list data."""

    def __init__(self, data: Any):
        print(bold(" Initializing Numeric Processor..."))
        print(f" {bold('Processing data:')} {data}")

    def process(self, data: Any) -> str:
        """Validates and transforms data (expected: list of int)."""
        try:
            data_list = list(data)
            try:
                valid_data = [int(x) for x in data_list]
                count = len(valid_data)
                total: int = sum(valid_data)
                avg: float = total / count if count > 0 else 0
                result = (f"Processed {count} numeric values, " +
                          f"sum={total}, avg={avg:.1f}")
            except ValueError as e:
                result = f"{e}"
        except TypeError as e:
            result = f"{e}"

        return f"{result}"

    def validate(self, data: Any) -> bool:
        """Checks if the input is a list of integers."""
        try:
            data_list = list(data)
            try:
                for x in data_list:
                    int(x)
                result = "Numeric data verified"
                return True
            except ValueError as e:
                result = f"{e}"
                return False
        except TypeError as e:
            result = f"{e}"
            return False
        finally:
            print(f" {bold('Validation:')} {result}")

    def format_output(self, result: str) -> str:
        """Overrides base method to match the requested output style."""
        return f" {bold('Output:')} {result}"


class TextProcessor(DataProcessor):
    """Processor specifically for string data."""

    def __init__(self, data: Any):
        print(bold(" Initializing Text Processor..."))
        print(f' {bold("Processing data:")} "{data}"')

    def process(self, data: Any) -> str:
        """Calculates character count and word count."""
        try:
            valid_data = str(data)
            char_count = len(valid_data)
            word_count = len(valid_data.split())
            result = (f"Processed text: {char_count} characters, " +
                      f"{word_count} words")
        except TypeError as e:
            result = f"{e}"

        return result

    def validate(self, data: Any) -> bool:
        """Checks if the input is a non-empty string."""
        try:
            str(data)
            result = "Text data verified"
            return True
        except TypeError as e:
            result = f" {e}"
            return False
        finally:
            print(f" {bold('Validation:')} {result}")

    def format_output(self, result: str) -> str:
        """Formats the final output string."""
        return f" {bold('Output:')} {result}"


class LogProcessor(DataProcessor):
    """Processor specifically for log message strings."""

    def __init__(self, data: Any):
        print(bold(" Initializing Log Processor..."))
        print(f' {bold("Processing data:")} "{data}"')
        self.valid_log = ["ERROR", "INFO"]

    def process(self, data: Any) -> str:
        """Parses the log level and the message content."""

        try:
            str(data)
            parts = data.split(":", 1)
            if len(parts) < 2:
                raise InvalidLogFormatError()
            log_type = parts[0].strip()
            if log_type not in self.valid_log:
                raise InvalidLogFormatError()
            if log_type == "ERROR":
                log_prefix = "[ALERT] ERROR"
            else:
                log_prefix = "[INFO] INFO"
            message = parts[1].strip()
            result = f"{log_prefix} level detected: {message}"
        except (TypeError, AttributeError, InvalidLogFormatError) as e:
            result = f"{e}"
        finally:
            return result

    def validate(self, data: Any) -> bool:
        """Checks if the input is a string containing a colon."""
        try:
            str(data)
            parts = data.split(":", 1)
            if len(parts) < 2:
                raise InvalidLogFormatError()
            log_type = parts[0].strip()
            if log_type not in self.valid_log:
                raise InvalidLogFormatError()
            result = "Log entry verified"
            return True
        except (TypeError, AttributeError, InvalidLogFormatError) as e:
            result = f"{e}"
            return False
        finally:
            print(f" {bold('Validation:')} {result}")

    def format_output(self, result: str) -> str:
        """Custom formatting to add the ALERT prefix for logs."""
        return f" {bold('Output:')} {result}"
