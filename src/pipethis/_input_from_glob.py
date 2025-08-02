"""
This module defines the `FromGlob` class, enabling recursive scanning of
directories and files matching specific glob patterns. Files are streamed
using customizable handlers, facilitating efficient file-based pipeline inputs.

It is useful for pipelines that need to process structured files across
multiple nested directories.

Example Usage:
    >>> from pipethis._input_from_glob import FromGlob
    >>> with FromGlob("/path/to/data/**/*.txt") as glob_input:
    ...     for line in glob_input.stream():
    ...         print(line.data)
"""

import os
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable, Type

from ._base import FileHandlerBase
from ._file_handler import TextFileHandler
from ._input_from_file import FromFile

class FromGlob:
    """
    Reads data by matching file paths using glob patterns.
    """

    def __init__(
            self,
            folder_path: Path | str,
            file_handler: Type[FileHandlerBase] | None = None,
            ignore_folders: list[str] | None = None,
            keep_patterns: list[str] | None = None,
            ignore_patterns: list[str] | None = None,
    ):
        """
        Initializes the `FromGlob` class to process and filter files from a directory
        based on glob patterns and custom file handlers.

        Globbing is a pattern-matching technique that resembles Unix shell-style
        wildcards. Supported patterns include:
        - `*` matches none or any number of characters (e.g., "*.txt" matches any `.txt` file).
        - `?` matches 1 character (e.g., "file?.txt" matches "file1.txt" but not "file01.txt").
        - `[...]` matches single character in brackets (e.g., "file[123].txt" matches "file1.txt").
        - Recursive wildcard matching (using `**`) matches files at any depth in the folder.

        Args:
            folder_path (Path | str): The root directory to search for files.
            file_handler (Type[FileHandlerBase] | None): A custom class for handling files.
                This class determines how files will be read or streamed. If not provided,
                the default registry-based handler will be used.
            ignore_folders (list[str] | None): A list of folder names to exclude from traversal.
                Folders with names matching these patterns will be skipped.
            keep_patterns (list[str] | None): A list of patterns to include in the results.
                Examples include `["*.txt", "*.csv"]`. If this is not empty, only files matching
                these patterns will be processed. Conflicts with `ignore_patterns`.
            ignore_patterns (list[str] | None): A list of patterns to exclude from the results.
                Examples include `["*.log", "*.tmp"]`. If this is set, files matching these
                patterns will be ignored.
                **Priority**: Ignore patterns take precedence over keep patterns.

        Raises:
            ValueError: If both `keep_patterns` and `ignore_patterns` are provided simultaneously.
        """

        self.folder_path = Path(folder_path) if isinstance(folder_path, str) else folder_path
        self.file_handler = file_handler or TextFileHandler
        self.keep_patterns = keep_patterns or []
        self.ignore_patterns = ignore_patterns or []
        self.ignore_folders = ignore_folders or []

        # Validate that both lists are not simultaneously set
        if self.keep_patterns and self.ignore_patterns:
            msg = "You can specify either keep_patterns or ignore_patterns, but not both."
            raise ValueError(msg)

        if not self.folder_path.exists():
            msg = f"Glob folder_path {self.folder_path} does not exist."
            raise ValueError(msg)

    def __enter__(self):
        """
        Enter the context. No real resource is acquired, but this makes
        FromGlob interchangeable with FromFile in context managers.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context. Nothing needs to be closed or cleaned up.
        """


    def stream(self) -> Iterable[Path]:
        """
            Traverse the folder and stream files that match the filtering criteria.

            The folder traversal uses `os.walk`, which provides a tree-like iteration over all
            directories and their subdirectories. For each directory:
            - Subdirectories listed in `ignore_folders` are skipped.
            - Files are checked against `keep_patterns` and `ignore_patterns` using glob matching.

            NOTE: os.walk is used because it is faster, it allows you to exclude folders without
            walking them.  When used this can dramatically speed up processing...at the cost
            of not using the rglob feature of pathlib.

            Globbing Mechanics:
            - Each file is matched against the patterns provided using the `fnmatch` module.
            - If a file matches an ignore pattern (in `ignore_patterns`), it is skipped even if
              it matches a keep pattern.
            - If `keep_patterns` is provided, files must match at least one pattern from this list.
            - If neither `keep_patterns` nor `ignore_patterns` is provided, all files are included.

            Yields:
                Path: Paths to files that satisfy the filtering rules, using the specified file
                handler (if provided) to process streamed data.

            Examples:
                1. Include `.txt` files, but ignore `.tmp` files:
                    >>> from_glob = FromGlob(folder_path="/data", keep_patterns=["*.txt"],
                                            ignore_patterns=["*.tmp"])
                    >>> list(from_glob.stream())
                    [Path('/data/file1.txt'), Path('/data/file2.txt')]

                2. Include all files in the folder, but skip logs and temporary files:
                    >>> from = FromGlob(folder_path="/data", ignore_patterns=["*.log", "*.tmp"])
                    >>> list(from.stream())
                    [Path('/data/file1.csv'), Path('/data/file2.json')]

                3. Skip specific subfolders while processing `.csv` files:
                    >>> from_glob = FromGlob(folder_path="/data",
                                            ignore_folders=["archive", "backup"],
                                            keep_patterns=["*.csv"])
                    >>> list(from_glob.stream())
                    [Path('/data/file1.csv'), Path('/data/subdir/file2.csv')]
            """

        for root, dirs, files in os.walk(self.folder_path):
            # Modify the 'dirs' list in-place (since it is mutable) to exclude ignored folders.
            # By using 'dirs[:]', we write into the original 'dirs' list that 'os.walk'
            # uses internally. This ensures that the ignored folders are skipped during
            # traversal in subsequent iterations.
            dirs[:] = [d for d in dirs if d not in self.ignore_folders]

            # Iterate through files in the remaining folders
            for file in files:
                file_path = Path(root, file)

                # Apply the matching rules
                if self._should_keep(file_path.name):
                    # Use context management for FromFile to handle resources
                    with FromFile(filepath=file_path,
                                  handler=self.file_handler) as from_file:
                        yield from from_file.stream()


    def _should_keep(self, filename: str) -> bool:
        """
        Checks if a file should be included based on `keep_patterns` and `ignore_patterns`.

        Args:
            filename (str): Name of the file.

        Returns:
            bool: True if the file passes all filters and should be kept, False otherwise.
        """
        # Ignore patterns take precedence: if a file matches an ignore pattern, skip it
        if any(fnmatch(filename, pattern) for pattern in self.ignore_patterns):
            return False

        # If keep patterns are specified, the file must match at least one
        if self.keep_patterns:
            return any(fnmatch(filename, pattern) for pattern in self.keep_patterns)

        # If no keep patterns are specified, include by default
        return True
