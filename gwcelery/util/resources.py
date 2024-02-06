"""Package data helpers."""
import json
from importlib import resources

__all__ = ('read_binary', 'read_json', 'read_text')


def read_binary(pkg, filename):
    """Load a binary file from package data."""
    return resources.files(pkg).joinpath(filename).read_bytes()


def read_json(pkg, filename):
    """Load a JSON file from package data."""
    with resources.files(pkg).joinpath(filename).open('r') as f:
        return json.load(f)


def read_text(pkg, filename):
    """Load a binary file from package data."""
    return resources.files(pkg).joinpath(filename).read_text()
