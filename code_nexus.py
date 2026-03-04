#!/usr/bin/env python3

"""
Code Nexus - Interactive Test Runner
Run this file to test your exercises interactively.
Usage: python3 main.py
"""

import os
import sys


"""Standard ANSI Color codes"""
R = "\033[1;91m"
G = "\033[1;92m"
Y = "\033[1;93m"
B = "\033[1;94m"
M = "\033[1;95m"
C = "\033[1;96m"
W = "\033[1;97m"
O = "\033[0m"


def div() -> None:
    """Prints a line divider."""
    print(" " + "-" * 60)


def add_exercise_folder_to_path(folder_name: str) -> None:
    """Adds the specified exercise folder to the Python path."""
    folder_path = os.path.join(os.path.dirname(__file__), folder_name)
    if folder_path not in sys.path:
        sys.path.insert(0, folder_path)


class CodeNexus:
    """Interactive test runner for Code Nexus exercises."""

    @staticmethod
    def ft_data_processor_foundation() -> None:
        """Tests Exercise 0: Data Processor Foundation."""
        try:
            add_exercise_folder_to_path("ex0")
            from stream_processor import (
                NumericProcessor,
                TextProcessor,
                LogProcessor,
            )

            div()
            print(f" {W}Exercise 0 - Data Processor Foundation{O}")
            div()

            samples = [
                (NumericProcessor(), [1, 2, 3, 4, 5]),
                (TextProcessor(), "Hello Nexus World"),
                (LogProcessor(), "ERROR: Connection timeout"),
            ]

            for processor, data in samples:
                print()
                if processor.validate(data):
                    result = processor.process(data)
                    print(processor.format_output(result))
                else:
                    print(f" {R}Validation failed for {data}{O}")

            print()
            div()
            print(f" {G}✅ Exercise 0 complete!{O}")
            div()

        except ImportError as e:
            print(f" {R}❌ Could not import Ex0 — {e}{O}")
        except Exception as e:
            print(f" {R}❌ Error in Ex0 — {e}{O}")

    @staticmethod
    def ft_polymorphic_streams() -> None:
        """Tests Exercise 1: Polymorphic Streams."""
        try:
            add_exercise_folder_to_path("ex1")
            from data_stream import (
                SensorStream,
                TransactionStream,
                EventStream,
                StreamProcessor,
            )

            div()
            print(f" {W}Exercise 1 - Polymorphic Streams{O}")
            div()

            streams = [
                (SensorStream("SENSOR_001"),
                 ["temp:22.5", "humidity:65", "pressure:1013"]),
                (TransactionStream("TRANS_001"),
                 ["buy:100", "sell:150", "buy:75"]),
                (EventStream("EVENT_001"),
                 ["login", "error", "logout"]),
            ]

            sp = StreamProcessor()
            for stream, batch in streams:
                print()
                print(sp.process_stream(stream, batch))

            print()
            div()
            print(f" {G}✅ Exercise 1 complete!{O}")
            div()

        except ImportError as e:
            print(f" {R}❌ Could not import Ex1 — {e}{O}")
        except Exception as e:
            print(f" {R}❌ Error in Ex1 — {e}{O}")

    @staticmethod
    def ft_nexus_integration() -> None:
        """Tests Exercise 2: Nexus Integration."""
        try:
            add_exercise_folder_to_path("ex2")
            from nexus_pipeline import (
                JSONAdapter,
                CSVAdapter,
                StreamAdapter,
                NexusManager,
            )

            div()
            print(f" {W}Exercise 2 - Nexus Integration{O}")
            div()

            manager = NexusManager()
            manager.add_pipeline(JSONAdapter("JSON_01"))
            manager.add_pipeline(CSVAdapter("CSV_01"))
            manager.add_pipeline(StreamAdapter("STREAM_01"))

            test_data = [
                '{"sensor": "temp", "value": 23.5, "unit": "C"}',
                "user,action,timestamp",
                ["22.1", "21.9", "22.5", "22.3", "21.8"],
            ]

            for data in test_data:
                print()
                print(manager.process_data(data))

            print()
            div()
            print(f" {G}✅ Exercise 2 complete!{O}")
            div()

        except ImportError as e:
            print(f" {R}❌ Could not import Ex2 — {e}{O}")
        except Exception as e:
            print(f" {R}❌ Error in Ex2 — {e}{O}")


def main() -> None:
    """Main entry point — shows interactive menu."""
    print()
    cd = CodeNexus()

    div()
    print(f" {W}🌐 Welcome to Code Nexus!{O}")
    div()
    print(" This helper will help you test the exercises of this module.")
    print(" Which exercise would you like to test?")
    print()

    print(f" {W}{'n.':<5}{'Exercise':<30}{'Description'}{O}")
    div()
    print(f" {'0':<5}{'Data Processor Foundation':<30}"
          "Base classes and method overriding")
    print(f" {'1':<5}{'Polymorphic Streams':<30}"
          "Adaptive data streams with inheritance")
    print(f" {'2':<5}{'Nexus Integration':<30}"
          "Enterprise pipeline architecture")
    print()

    choice = input(f"{W} 🌐 Enter your choice (0/1/2): {O}")

    if choice == "0":
        cd.ft_data_processor_foundation()
    elif choice == "1":
        cd.ft_polymorphic_streams()
    elif choice == "2":
        cd.ft_nexus_integration()
    else:
        print(f" {R}❌ Invalid choice! Please enter 0, 1, or 2{O}")

    print()


if __name__ == "__main__":
    main()
