import pathlib

from ._base import FileHandlerBase
from ._streamitem import LineStreamItem


class TextFileHandler(FileHandlerBase):
    """
    Handles streaming text files line by line.
    Supports context management to open and close file resources.
    """

    def __init__(self, file_path: pathlib.Path, encoding='utf-8'):

        # Delay import yuck, to prevent circular imports.
        super().__init__(file_path)
        self._file = None  # Internal file resource
        self.encoding = encoding

    def __enter__(self):
        """
        Open the file for reading.
        """
        self._file = self.file_path.open('r', encoding=self.encoding)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the file.
        """
        if self._file:
            self._file.close()
            self._file = None

    def stream(self):
        """
        Stream lines from the opened file as LineStreamItems.
        """
        if not self._file:
            raise RuntimeError("The file is not open. You must use this file_handler in a context manager.")

        for sequence_id, line in enumerate(self._file, start=1):
            yield LineStreamItem(sequence_id, str(self.file_path), line.strip())
