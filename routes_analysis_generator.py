#!/usr/bin/env python

"""
# Example working with large routing table file
profiling the difference between a generator and a for loop for processing the routes

# download routes files from here
https://www.routeviews.org/routeviews/archive/
get the FRR format (MRT data)

# convert the MRT data to text file like this
https://github.com/bgpkit/bgpkit-parser
cargo install bgpkit-parser --features cli
note: this is a large file 3Gb
bgpkit-parser rib.20250701.0000.bz2 > all_routes.txt
"""

import cProfile
import logging
import os
import tracemalloc
from typing import Generator, List

# Ensure we find the file relative to this script
script_dir = os.path.dirname(os.path.abspath(__file__))
routes_file = os.path.join(script_dir, "all_routes.txt")


def stream_route_data(filepath: str) -> Generator[str, None, None]:
    """
    A generator that streams lines from a BGP RIB file
    """
    try:
        with open(filepath) as fin:
            for line in fin:
                yield line.strip()
    except FileNotFoundError:
        logging.error(f"couldnt open file: {filepath}")
    except Exception as err:
        logging.error(f"stream_route_data: error|{err}")


def search_routes_for_loop(search: str) -> List[str]:
    """
    Example function to process the routes file via a for loop
    """

    logging.info(f"process_routes|starting")
    result = []
    for line in stream_route_data(filepath=routes_file):
        if search in line:
            result.append(line)
    return result


def search_routes_generator(search: str) -> Generator[str, None, None]:
    """
    Example function to process the routes file via streaming generator
    """

    logging.info(f"process_routes|starting")
    for line in stream_route_data(filepath=routes_file):
        if search in line:
            yield line


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Time Profiling (cProfile) ===")
    print("\n--- cProfile: search_routes_for_loop ---")
    # profile using the for loop, searching for ASN 36351
    cProfile.run('search_routes_for_loop(search="36351")')

    print("\n--- cProfile: search_routes_generator ---")
    # profile using the generator, searching for ASN 36351
    # Note: we need to consume the generator for the code to actually execute inside
    cProfile.run('[x for x in search_routes_generator(search="36351")]')

    print("\n=== Memory Profiling (tracemalloc) ===")

    print("\n--- Testing search_routes_for_loop (List) ---")
    tracemalloc.start()

    # Run the list-based function
    t1 = search_routes_for_loop(search="36351")

    current1, peak1 = tracemalloc.get_traced_memory()
    print(f"Current memory usage: {current1 / 10**6:.2f} MB")
    print(f"Peak memory usage:    {peak1 / 10**6:.2f} MB")
    print(f"Result count: {len(t1)}")

    tracemalloc.stop()
    del t1  # cleanup

    print("\n--- Testing search_routes_generator (Generator) ---")
    tracemalloc.start()

    # Run the generator-based function (iterating without storing all)
    count = 0
    for route in search_routes_generator(search="36351"):
        count += 1

    current2, peak2 = tracemalloc.get_traced_memory()
    print(f"Current memory usage: {current2 / 10**6:.2f} MB")
    print(f"Peak memory usage:    {peak2 / 10**6:.2f} MB")
    print(f"Result count: {count}")

    tracemalloc.stop()

    print(
        "\n--- Peak memory comparison - search_routes_for_loop vs search_routes_generator---"
    )
    print(
        f"The generator function uses {peak2*100/peak1:.2f}% as much memory as the for loop"
    )
