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
        msg = lineinfo.line
        self.update_size(msg)
        print(lineinfo.line)


class ToFile(OutputBase):
    def __init__(self, file_name=None, mode='w', encoding="utf-8"):
        """
        Initialize the standard output writer.

        Args:
            file_name (str, optional): If provided, write output to the file with this name.
                                       If None, write to stdout.
        """
        super().__init__()
        self.file_name = file_name
        self.file = open(file_name, mode="w", encoding=encoding)

    def write(self, lineinfo:LineInfo):
        """
        Write the output of a LineInfo object to either stdout or a file.

        Args:
            lineinfo (LineInfo): The LineInfo object to write.
        """
        msg = lineinfo.line + "\n"
        self.file.write(msg)
        self.update_size(msg)

    def close(self):
        """
        Close the file if writing to a file.
        """
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
        msg = lineinfo.line + '\n'
        self.update_size(msg)
        self.text_output += str(msg)
