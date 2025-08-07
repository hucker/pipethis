"""
This module defines the `ToFile` class, enabling output to be written to disk.
The class supports configurable file modes (e.g., `append` or `overwrite`)
and encoding, making it suitable for saving pipeline results in various formats.

This component integrates seamlessly with pipelines as an output step.

Example Usage:
    >>> from pipethis._output_to_file import ToFile
    >>> with ToFile("output.txt", mode="w") as file_output:
    ...     for item in input_stream.stream():
    ...         file_output.write(item)
"""

from ._base import OutputBase
from ._logging import get_logger
from ._streamitem import LineStreamItem

# Create local logger
logger = get_logger(__name__)


class ToFile(OutputBase):
    """
    A class to handle output directed to a file.

    This class writes output to a file specified by the user, with customizable
    file mode and encoding.
    """

    def __init__(self, file_name=None, mode='w', encoding="utf-8"):
        """
        Initialize the file writer.

        Args:
            file_name (str, optional): The name of the file to which output will be written.
                                       Defaults to None, in which case output is directed to stdout.
            mode (str, optional): The mode in which the file is opened. Defaults to 'w'.
            encoding (str, optional): The file encoding. Defaults to "utf-8".
        """
        super().__init__()
        logger.debug("Init ToFile with path: %s", file_name)
        self.file_name = file_name
        self.mode = mode
        self.encoding = encoding
        self.file = None  # File will be opened in context

    def __enter__(self):
        """Open the file and return the instance."""
        self.file = open(self.file_name, mode=self.mode, encoding=self.encoding)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Ensure the file is closed properly on exit."""
        self.close()

    def write(self, lineinfo: LineStreamItem):
        """
        Write the output of a LineInfo object to a file.

        Args:
            lineinfo (LineStreamItem): The LineInfo object to write.
        """
        msg = lineinfo.data + "\n"
        self.file.write(msg)

    def close(self):
        """
        Close the file if writing to a file.
        """
        if self.file:
            self.file.close()
