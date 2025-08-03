"""
This module defines the `TextFileHandler` class, which provides functionality for
streaming text files line by line in a structured and memory-efficient way.

The `TextFileHandler` class:
- Extends the `FileHandlerBase` to handle text file processing.
- Supports context management for safe resource handling (opening and closing files).
- Streams each line from a text file as a `LineStreamItem` object, which includes
  metadata like line number, file path, and the actual line content.

Target Use Case:
This handler is ideal for file processing tasks where text lines are consumed
sequentially, such as processing logs, configuration files, or structured text data.

Example Usage:
    ```python
    from pathlib import Path

    file_handler = TextFileHandler(Path("/path/to/file.txt"))
    with file_handler as handler:
        for item in handler.stream():
            print(f"{item.sequence_id}: {item.data}")
    ```
"""

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
            msg = "The file is not open. You must use this file_handler in a context manager."
            raise RuntimeError(msg)

        for sequence_id, line in enumerate(self._file, start=1):
            line = line.replace('\r\n', '\n').replace('\r', '\n').replace('\n', '')

            yield LineStreamItem(sequence_id, str(self.file_path), line)
