from pep249 import *
from pybitable.dbapi import *

__version__ = "0.0.1"


def connect(connection_string: str = "", read_only=False) -> Connection:
    """Connect to a Lark Base, returning a connection."""
    return Connection(connection_string, read_only=read_only)

