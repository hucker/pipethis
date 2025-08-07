"""
This module provides the `FromString` class for streaming data from a single
string. The input string is split into smaller chunks based on a separator
(e.g., lines, words) and yielded as `LineStreamItem` objects.

This is particularly useful for testing or processing in-memory text data.

Example Usage:
    >>> from pipethis._input_from_string import FromString
    >>> input_data = 'line1\nline2\nline3'
    >>> input_stream = FromString(input_data, sep="\n")
    >>> for item in input_stream.stream():
    ...     print(item.sequence_id, item.data)
"""
from typing import Iterable

from ._base import InputBase
from ._logging import get_logger
from ._streamitem import LineStreamItem

# Create local logger
logger = get_logger(__name__)


class FromString(InputBase):
    """
    Stream the input string as `LineStreamItem` objects.

    This method splits the provided string into smaller chunks using the specified sep
    and associates each chunk with a sequence ID and a resource name. Each chunk is yielded
    as a `LineStreamItem` object.

    Yields:
        LineStreamItem: An object representing a single chunk of text, with metadata
        such as its sequence ID and the resource name.

    Example:
        >>> text = "line1,line2,line3"
        >>> from_string = FromString(text, sep=",", name="example")
        >>> for item in from_string.stream():
        ...     print(item.sequence_id, item.data)
        1 line1
        2 line2
        3 line3
    """

    def __init__(self, text: str, sep='\n', name='text'):
        """
        Initialize the FromString instance.

        Args:
            name (str): Name of the string source.
            text (str): The string data to stream from.
            sep (str, optional): Separator for splitting string into lines. Defaults to '\n'.
        """
        self.name = name
        self.text = text
        self.sep = sep

    def __enter__(self):
        """
        Enter the context. No real resource is acquired, but this makes
        FromString interchangeable with FromFile in context managers.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context. Nothing needs to be closed, but this makes
        FromString compatible with context management.
        """

    def stream(self) -> Iterable[LineStreamItem]:
        """Stream lines split by the specified sep."""
        for line_number, data in enumerate(self.text.split(self.sep), start=1):
            yield LineStreamItem(sequence_id=line_number, resource_name=self.name, data=data)
