import argparse
from typing import List

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def parse_command_line(command_line: List[str]) -> argparse.Namespace:
    # TODO : Usage should show program name not __main__.py
    parser = argparse.ArgumentParser(description="Compare two datasets.", add_help=False)
    parser.add_argument(
        "-f",
        "--file",
        dest="filename",
        nargs="?",
        type=str,
        required=False,
        help="Config file to use)",
    )
    parser.add_argument(
        "-s",
        "--search_diff",
        dest="search_diff",
        nargs="?",
        type=bool,
        required=False,
        help="search for rows)",
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        dest="loglevel",
        nargs="?",
        type=str,
        required=False,
        choices=LOG_LEVELS,
        help="Log level)",
    )
    parser.add_argument(
        "--help",
        action="help",
        help="show this help message and exit",
    )
    parser.set_defaults(
        filename="default_config.yaml",
        search_diff=False,
        loglevel="INFO",
    )
    return parser.parse_args(command_line)
