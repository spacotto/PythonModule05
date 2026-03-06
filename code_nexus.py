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
            numeric1: list = [1, 2, 3]

            # Collect data for Text Processor
            text = input(f" {W}Enter a text string: {O}")
            text1 = "Hello World!"

            # Test str for Log Processor
            log = input(f" {W}Enter log (format: ERROR/INFO: Msg): {O}")
            # log_err = "ERROR: Connection timeout"
            log_info = "INFO: System ready"

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
            print(f" {W}Processing multiple data types through same interface...{O}")  # noqa: E501
            r1 = np.process(numeric1)
            print(f" {W}Result 1:{O} {r1}")
            r2 = tp.process(text1)
            print(f" {W}Result 2:{O} {r2}")
            r3 = lp.process(log_info)
            print(f" {W}Result 3:{O} {r3}")

            print()
            print(f" {W}Foundation systems online. Nexus ready for advanced streams.{O}")  # noqa: E501

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
                DataStream,
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

            # Collect data for Sensor Stream 
            s = input(f" {W}Enter Sensor Data: {O}")

            try:
                sensor: list = [int(x) for x in s.split()]
            
            except Exception as e:
                sensor: list = s.split()
                print(" Invalid input (3 int). Testing raw input.")

            # Collect data for Transaction Stream            
            transactions: list = ["buy", "sell"]
            trans = []
            t = input(f" {W}Enter Transaction Data: {O}")

            try:
                for _ in range(int(t)):
                    action = random.choice(transactions)
                    amount = random.randint(100, 1000)
                    trans.append(f"{action}:{amount}")

            except Exception as e:
                trans = t.split()
                print(" Invalid input (int). Testing raw input.")

            # Collect data for Event Stream
            events: list = ["error", "login", "logout"]
            event = []
            en = input(f" {W}Enter Event Data: {O}")

            try:
                for _ in range(int(en)):
                    action = random.choice(events)
                    trans.append(f"{random.choice(events)}")

            except Exception as e:
                event = t.split()
                print(" Invalid input (int). Testing raw input.")

            # Sensor Stream Test
            print()
            ss = SensorStream(stream_id)
            print(ss.process_batch(sensor))

            # Transaction Stream Test
            print()
            ts = TransactionStream(stream_id)
            print(ts.process_batch(trans))

            # Event Stream Test
            print()
            es = EventStream(stream_id)
            print(es.process_batch(event))

            print()
            print(f" {W}Polymorphic Stream Processing{O}")
            div()
            print(f" {W}Processing mixed stream types through unified interface...{O}")
            print()
            print(f" {W}All streams processed successfully. Nexus throughput optimal.{O}")

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
