"""
This module contains output handlers for processing LineStreamItem objects.

The classes in this module provide implementations for writing output to different destinations,
such as stdout, a file, or an in-memory string.
"""

from ._base import OutputBase
from ._streamitem import LineStreamItem


class ToStdOut(OutputBase):
    """
    A class to handle output directed to the standard output stream.
    """

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
