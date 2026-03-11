#!/usr/bin/env python3

"""
Code Nexus - Interactive Test Runner
Run this file to test your exercises interactively.
Usage: python3 main.py
"""

import os
import random
import sys


"""Standard ANSI Color codes"""
R = "\033[1;91m"
G = "\033[1;92m"
Y = "\033[1;93m"
B = "\033[1;94m"
M = "\033[1;95m"
C = "\033[1;96m"
W = "\033[1;97m"
O = "\033[0m"  # noqa: E741


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

            print()
            div()
            print(f" {W}CODE NEXUS - DATA PROCESSOR FOUNDATION{O}")
            div()

            # Collect data for Numeric Processor
            raw = input(f" {W}Enter numbers separated by spaces: {O}")
            numeric: list = raw.split()

            # Collect data for Text Processor
            text = input(f" {W}Enter a text string: {O}")

            # Test str for Log Processor
            log = input(f" {W}Enter log (format: ERROR/INFO: Msg): {O}")

            # Numeric Processor Test
            print()
            np = NumericProcessor(numeric)
            result = np.process(numeric)
            np.validate(numeric)
            print(np.format_output(result))

            # Text Processor Test
            print()
            tp = TextProcessor(text)
            result = tp.process(text)
            tp.validate(text)
            print(tp.format_output(result))

            # Log Processor Test
            print()
            lp = LogProcessor(log)
            result = lp.process(log)
            lp.validate(log)
            print(lp.format_output(result))

            # Test all together
            print()
            print(f" {W}Polymorphic Processing Demo{O}")
            div()
            print(f" {W}Processing multiple data types" +
                  f" through same interface...{O}")
            print(f" {W}Result 1:{O} {np.process(numeric)}")
            print(f" {W}Result 2:{O} {tp.process(text)}")
            print(f" {W}Result 3:{O} {lp.process(log)}")

            print()
            print(f" {W}Foundation systems online." +
                  f" Nexus ready for advanced streams.{O}")

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

            # Collect and set stream_id
            stream_id = input(f" {W}Enter Stream ID: {O}")

            # Initialise the batch of data where all gen data will be stored
            data_batch: list = []

            # Collect and add data for Sensor Stream
            valid_sensors: list = ["temperature", "humidity", "pressure"]
            s = input(f" {W}Sensor Data: How many entries?{O} ")

            ranges = {"temperature": (15, 60),
                      "humidity": (20, 100),
                      "pressure": (980, 1060)}

            try:
                for _ in range(int(s)):
                    sensor = random.choice(valid_sensors)
                    low, high = ranges[sensor]
                    reading_value = random.randint(low, high)
                    data_batch.append(f"{sensor}:{reading_value}")

            except Exception as e:
                data_batch.extend(s.split())
                print(f" {e}")
                print(" Invalid generator input (int). Testing raw input.")

            # Collect and add data for Transaction Stream
            valid_trans: list = ["buy", "sell"]
            t = input(f" {W}Transaction Data: How many entries?{O} ")

            try:
                for _ in range(int(t)):
                    action = random.choice(valid_trans)
                    amount = random.randint(100, 1000)
                    data_batch.append(f"{action}:{amount}")

            except Exception as e:
                data_batch.extend(t.split())
                print(f" {e}")
                print(" Invalid generator input (int). Testing raw input.")

            # Collect and add data for Event Stream
            valid_events: list = ["error", "login", "logout"]
            n = input(f" {W}Event Data: How many entries?{O} ")

            try:
                for _ in range(int(n)):
                    data_batch.append(f"{random.choice(valid_events)}")

            except Exception as e:
                data_batch.extend(n.split())
                print(f" {e}")
                print(" Invalid generator input (int). Testing raw input.")

            # System Demo
            print()
            print(f"{W} CODE NEXUS: POLYMORPHIC STREAM SYSTEMI{O}")
            div()

            ss = SensorStream(stream_id)
            ss.process_batch(data_batch)
            ss.get_stats()
            ss.display_stats()
            print()

            ts = TransactionStream(stream_id)
            ts.process_batch(data_batch)
            ts.get_stats()
            ts.display_stats()
            print()

            es = EventStream(stream_id)
            es.process_batch(data_batch)
            es.get_stats()
            es.display_stats()
            print()

            # Polymorphic Stream Processing
            sp = StreamProcessor()
            sp.add_stream(ss)
            sp.add_stream(ts)
            sp.add_stream(es)

            print(f"{W} Polymorphic Stream Processing{O}")
            div()
            sp.display_analysis(data_batch)

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

            # Collect data
            json_data = input(f" {W}Enter JSON data: {O}")
            csv_data = input(f" {W}Enter CSV data: {O}")
            stream_data = input(f" {W}Enter Stream data: {O}")

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
