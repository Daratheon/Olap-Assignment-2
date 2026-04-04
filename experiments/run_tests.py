import subprocess
import sys

import config # to make src available for import

TEST_SCRIPTS = [
    "experiments/test_zone_map.py",
    "experiments/test_bitmap_index.py",
    "experiments/test_rle.py",
    "experiments/test_dictionary.py",
    "experiments/test_delta_bitpack.py",
    "experiments/test_bit_sliced_index.py",
    "experiments/test_column_imprints.py",
    "experiments/test_mostly.py",
]


def main():
    print("\n=== Running All OLAP Engine Tests ===\n")

    for script in TEST_SCRIPTS:
        print(f"\n--- Running {script} ---\n")

        result = subprocess.run(
            [sys.executable, script],
            capture_output=True,
            text=True
        )

        print(result.stdout)

        if result.returncode != 0:
            print("ERROR:")
            print(result.stderr)
            return

    print("\n=== All tests completed successfully ===")


if __name__ == "__main__":
    main()
