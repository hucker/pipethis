from _base import OutputBase
from _lineinfo import LineInfo


class ToStdOut(OutputBase):

    def write(self, lineinfo:LineInfo):
        """
        Write the output of a LineInfo object to either stdout or a file.

        Args:
            lineinfo (LineInfo): The LineInfo object to write.
        """
        super().__init__()
        msg = lineinfo.data
        self.update_size(msg)
        print(lineinfo.data)


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

    def write(self, lineinfo:LineInfo):
        """
        Write the output of a LineInfo object to either stdout or a file.

        Args:
            lineinfo (LineInfo): The LineInfo object to write.
        """
        msg = lineinfo.data + "\n"
        self.file.write(msg)
        self.update_size(msg)

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

    def write(self, lineinfo:LineInfo):
        """
        Write the output of a LineInfo object to a string.

        Args:
            lineinfo (LineInfo): The LineInfo object to write.
        """
        msg = lineinfo.data + '\n'
        self.update_size(msg)
        self.text_output += str(msg)
