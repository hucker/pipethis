"""
This module provides the `FromFolder` class for reading and streaming files from
a specified directory. It can filter files based on inclusion or exclusion patterns
and allows users to define custom file handlers to process different types of files.

Ideal for batch processing scenarios, such as processing a folder of log files
or structured data files in various formats.

Example Usage:
    >>> from pipethis._input_from_folder import FromFolder
    >>> with FromFolder("/path/to/logs", keep_patterns=["*.log"]) as folder:
    ...     for line in folder.stream():
    ...         print(line.data)
"""

import pathlib
from typing import Type

from ._input_from_file import FromFile
from ._file_handler import TextFileHandler
from ._base import FileHandlerBase,InputBase


class FromFolder(InputBase):
    """
    Reads and processes data from files in a folder, with support for file filtering
    based on inclusion or exclusion patterns.

    This class is part of the pipeline's input components. It iterates through files
    in a specified folder, dynamically applying handlers to process each file.

    Features:
    - Filters files using `keep_patterns` (inclusion patterns) or `ignore_patterns`
      (exclusion patterns).
    - Supports custom file handlers to process specific file formats.

    Attributes:
        folder_path (pathlib.Path): Path to the folder containing files to be processed.
        file_handler (Type[FileHandlerBase]): The file handler class used to process files.
                                              Defaults to `TextFileHandler` if not specified.
        keep_patterns (list[str]): Inclusion patterns for filtering (e.g., ['*.txt', '*.py']).
        ignore_patterns (list[str]): Exclusion patterns for filtering (e.g., ['*.log', '*.tmp']).

    Raises:
        ValueError: If both `keep_patterns` and `ignore_patterns` are provided simultaneously.
    """

    def __init__(
            self,
            folder_path: pathlib.Path | str,
            file_handler: Type[FileHandlerBase] | None = None,
            keep_patterns: list[str] | str | None = None,
            ignore_patterns: list[str] | str | None = None,
    ):
        """
        Initializes the `FromFolder` instance.

        This method sets up the folder path, file handler, and optional file filtering
        patterns for processing files in a directory. If no file handler is explicitly
        provided, the default `TextFileHandler` is used.

        Args:
            folder_path (pathlib.Path|str): Path to the folder containing files to process.
            file_handler (Type[FileHandlerBase] | None): Custom file handler class for processing
                                                         files.  Defaults to `TextFileHandler`.
            keep_patterns (list[str] | None): List of inclusion patterns for filtering files.
                                              Only files matching these patterns will be processed.
                                              Examples: ['*.txt', '*.csv'].
            ignore_patterns (list[str] | None): List of exclusion patterns for filtering files.
                                                Files matching these patterns will be ignored.
                                                Examples: ['*.log', '*.tmp'].

        Raises:
            ValueError: If both `keep_patterns` and `ignore_patterns` are provided simultaneously.
        """
        self.folder_path = pathlib.Path(folder_path)
        self.file_handler = file_handler or TextFileHandler
        self.keep_patterns = self._list_or_string(keep_patterns)
        self.ignore_patterns =  self._list_or_string(ignore_patterns)

        # Validate that both lists are not simultaneously set
        if self.keep_patterns and self.ignore_patterns:
            msg = "You can specify either keep_patterns or ignore_patterns, but not both."
            raise ValueError(msg)

    def __enter__(self):
        """
        Enter the context for file handling.

        This method allows `FromFolder` objects to be used as part of a context
        manager. While no real resource acquisition is required in this class,
        it enables seamless interoperability within pipeline code that uses context management.

        Returns:
            FromFolder: The current `FromFolder` instance.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context for file handling.

        Cleans up resources upon exiting the context. Since this class does not
        require explicit resource management, this method simply facilitates uniform
        behavior across pipeline components using context managers.

        Args:
            exc_type (type): Exception type, if raised within the context.
            exc_value (Exception): Exception instance, if raised within the context.
            traceback (Traceback): Traceback object, if an exception is raised.
        """


    def stream(self):
        """
        Stream data from files in the folder.

        Iterates through the files in the specified folder, dynamically applying the
        file handler to each file. Files are filtered based on the inclusion or exclusion
        patterns, if specified. The resulting stream items are yielded one by one.

        Yields:
            StreamItem: A stream item generated by the dynamically applied file handler.

        Example Usage:
            ```python
            folder = Path("/path/to/folder")
            for item in FromFolder(folder).stream():
                print(item)
            ```
        """
        for file_path in self.folder_path.iterdir():
            if not self._should_include(file_path):
                continue

            with FromFile(filepath=file_path, handler=self.file_handler) as from_file:
                yield from from_file.stream()

    def _should_include(self, file_path: pathlib.Path) -> bool:
        """
        Determine whether a file should be included in the processing.

        This private helper method checks the inclusion (`keep_patterns`) and
        exclusion (`ignore_patterns`) patterns to decide whether the given file
        is eligible for processing.

        Files and folders are evaluated in the following sequence:
        1. Directories are automatically excluded.
        2. Files are included based on `keep_patterns` if specified.
        3. Files matching `ignore_patterns` are excluded if specified.

        Args:
            file_path (pathlib.Path): The path of the file to evaluate.

        Returns:
            bool: True if the file should be included; False otherwise.
        """
        # Skip directories
        if file_path.is_dir():
            return False

        # Check keep_patterns (if specified)
        if self.keep_patterns:
            if not any(file_path.match(pattern) for pattern in self.keep_patterns):
                return False

        # Check ignore_patterns (if specified)
        if self.ignore_patterns:
            if any(file_path.match(pattern) for pattern in self.ignore_patterns):
                return False

        return True
