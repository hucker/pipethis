"""
This module provides the `ToStdOut` class for directing pipeline output to
the standard output stream. This is useful for debugging or interactive
pipeline testing, where the processed data is printed to the console.

The class adheres to the `OutputBase` interface and integrates seamlessly with
pipelines as an output component.

Example Usage:
    >>> from pipethis._output_to_stdout import ToStdOut
    >>> output = ToStdOut()
    >>> for item in input_stream.stream():
    ...     output.write(item)
"""

from ._base import OutputBase
from ._logging import get_logger
from ._streamitem import LineStreamItem

# Create local logger
logger = get_logger(__name__)


class ToStdOut(OutputBase):
    """
    A class to handle output directed to the standard output stream.
    """

    def __init__(self):
        super().__init__()
        logger.debug("Init ToStdOut")

    def write(self, lineinfo: LineStreamItem):
        """
        Write the output of a LineInfo object to either stdout or a file.

        Args:
            lineinfo (LineStreamItem): The LineInfo object to write.
        """
        super().__init__()
        print(lineinfo.data)

    def __enter__(self):
        """
        Enter the runtime context. Typically used to perform setup operations.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context. Typically used to perform cleanup operations.

        Args:
            exc_type (type): Exception type, if an exception occurred.
            exc_value (Exception): Exception instance, if an exception occurred.
            traceback (Traceback): Traceback object for the exception.
        """
