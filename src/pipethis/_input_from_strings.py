"""
This module defines the `FromStrings` class, which allows streaming data from
a list of strings. Each string is split into chunks based on a specified separator
and streamed as `LineStreamItem` objects.

This class supports processing multiple distinct text inputs in a single pipeline,
making it versatile for in-memory testing or multiple related text sources.

Example Usage:
    >>> from pipethis._input_from_strings import FromStrings
    >>> input_data = ["first string", "second string"]
    >>> input_stream = FromStrings(input_data, sep=" ")
    >>> for item in input_stream.stream():
    ...     print(item.sequence_id, item.data)
"""

from typing import Iterable

from ._base import InputBase
from ._input_from_string import FromString
from ._logging import get_logger
from ._streamitem import LineStreamItem

# Create local logger
logger = get_logger(__name__)


class FromStrings(InputBase):
    """
    A class to handle the streaming of multiple input strings, splitting each string
    into smaller chunks based on a specified sep, and yielding them as
    `LineStreamItem` objects.

    This class serves as an abstraction for processing multiple strings or a single string,
    allowing for efficient line-by-line streaming. It also supports context management,
    making it compatible with `with` statements.

    Attributes:
        name (str): A name identifier for the resource, included in each `LineStreamItem`.
        lines (list[str]): A list of input strings to process. If a single string is provided,
            it will be converted into a list containing that string.
        sep (str): The sep used to split each string into smaller chunks. Defaults to '\n'.
    """

    def __init__(self, lines: list[str] | str, sep: str = '\n', name: str = 'text'):
        """
        Initialize the FromStrings instance.

        Args:
            lines (list[str] | str): A list of strings or a single string to be processed.
                                     If a single string is provided, it will be wrapped in a list.
            sep (str, optional): The separator used to split the strings. Defaults to '\n'.
            name (str, optional): A unique name for the resource, used in the `LineStreamItem`.
                                  Defaults to 'text'.
        """
        if isinstance(lines, str):
            lines = [lines]

        self.name = name
        self.lines = lines
        self.sep = sep

    def __enter__(self):
        """
        Enter the context for the `FromStrings` instance.

        This method does not acquire any real resources but makes the `FromStrings`
        class interchangeable with file-based classes like `FromFile` for use in
        context managers. It ensures syntactical compatibility when used with `with` statements.

        Returns:
            FromStrings: The instance of itself.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context for the `FromStrings` instance.

        This method performs no specific operations since there are no open resources
        to close. However, it is implemented to maintain compatibility with context
        management protocols.
        """

    def stream(self) -> Iterable[LineStreamItem]:
        """
        Stream the processed strings as `LineStreamItem` objects.

        This method iterates over the input strings, splitting each string into smaller chunks
        based on the provided sep. Each chunk is yielded as an instance of `LineStreamItem`.
        The `sequence_id` of the `LineStreamItem` corresponds to the chunk's position in the
        original string, and the `resource_name` is augmented with an identifier for each
        input string.

        Yields:
            LineStreamItem: An item representing a single chunk of text and associated metadata.

        Example:
            >>> lines = ["This is a test.", "Another example line."]
            >>> from_strings = FromStrings(lines, sep=' ', name='test')
            >>> for item in from_strings.stream():
            ...     print(item.sequence_id, item.data)
            1 This
            2 is
            3 a
            4 test.
            1 Another
            2 example
            3 line.
        """
        for id_, line in enumerate(self.lines, start=1):
            # Use the generator returned by FromString to handle separation and streaming
            from_string = FromString(line, sep=self.sep, name=f"{self.name}-{id_}").stream()
            yield from from_string
