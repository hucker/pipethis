"""
This module implements the `ToString` class, a pipeline output that concatenates
processed data into a single in-memory string. It is helpful for capturing
pipeline output without writing to a file or stdout.

The class adheres to the `OutputBase` interface and is compatible with context
management for clean initialization and teardown.

Example Usage:
    >>> from pipethis._output_to_string import ToString
    >>> output = ToString()
    >>> for item in input_stream.stream():
    ...     output.write(item)
    >>> print(output.text_output)
"""

from ._base import OutputBase
from ._logging import get_logger
from ._streamitem import LineStreamItem

# Create local logger
logger = get_logger(__name__)


class ToString(OutputBase):
    """
    A class to handle output directed to an in-memory string.

    This class provides functionality for concatenating output to a single
    in-memory string.
    """

    def __init__(self):
        """
        Initialize the string writer.
        """
        super().__init__()
        logger.debug("Init ToString")
        self.text_output = ""  # This will store the concatenated string

    def write(self, lineinfo: LineStreamItem):
        """
        Write the output of a LineInfo object to a string.

        Args:
            lineinfo (LineStreamItem): The LineInfo object to write.
        """
        msg = lineinfo.data + '\n'
        self.text_output += str(msg)

    def __enter__(self):
        """
        Enter the runtime context. Typically used to perform setup operations.

        Clears the text_output to ensure it starts fresh when entering the context.
        """
        self.text_output = ""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context. Typically used to perform cleanup operations.

        Args:
            exc_type (type): Exception type, if an exception occurred.
            exc_value (Exception): Exception instance, if an exception occurred.
            traceback (Traceback): Traceback object for the exception.
        """
