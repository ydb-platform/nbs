from typing import Dict, Any
import pprint
import sys
import argparse
import os
import sys


# Our global parser that we will collect arguments into
parser = argparse.ArgumentParser()

# Global configuration dictionary that will contain parsed arguments
# It is also this variable that modules use to access parsed arguments
config: Dict[str, Any] = {}


def add_parser(title: str, description: str = ""):
    """Create a new context for arguments and return a handle."""
    return parser.add_argument_group(title, description)

def parser_args():
    """update arguments config variable and return the result"""
    config.update(vars(parser.parse_args()))
    return config

