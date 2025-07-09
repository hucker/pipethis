from _base import OutputBase
from _streamitem import LineStreamItem


class ToStdOut(OutputBase):

    def write(self, lineinfo:LineStreamItem):
        """
        Write the output of a LineInfo object to either stdout or a file.

        Args:
            lineinfo (LineStreamItem): The LineInfo object to write.
        """
        super().__init__()
        msg = lineinfo.data
        print(lineinfo.data)

    def __enter__(self):
        """
        Enter the runtime context. Typically used to perform setup operations.
        """
        # Clear text_output to ensure a fresh start in a context
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context. Typically used to perform cleanup operations.

        Args:
            exc_type (type): Exception type, if an exception occurred.
            exc_value (Exception): Exception instance, if an exception occurred.
            traceback (Traceback): Traceback object for the exception.
        """
        # For the purpose of a context manager, there's usually no cleanup needed here.
        # But if there were, you could perform it now.
        pass

class ToFile(OutputBase):
    def __init__(self, file_name=None, mode='w', encoding="utf-8"):
        """
        Initialize the file writer.

        Args:
            file_name (str, optional): If provided, write output to the file with this name.
                                       If None, write to stdout.
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

    def write(self, lineinfo:LineStreamItem):
        """
        Write the output of a LineInfo object to either stdout or a file.

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

    def __init__(self):
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
        """
        # Clear text_output to ensure a fresh start in a context
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
        # For the purpose of a context manager, there's usually no cleanup needed here.
        # But if there were, you could perform it now.
        pass
