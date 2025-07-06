from pathlib import Path
from typing import Iterable, List

from _base import InputBase
from _lineinfo import LineInfo


class FromFile(InputBase):
    def __init__(self, filepath: Path):
        self.filepath = filepath

    def stream(self) -> Iterable[LineInfo]:
        with open(self.filepath) as file:
            for line_number, line in enumerate(file, start=1):
                yield LineInfo(sequence_id=line_number, resource_name=str(self.filepath), data=line.strip())


class FromFolder:
    def __init__(self, folder_path: Path, keep_extensions: List[str] = None, ignore_extensions: List[str] = None):
        """
        Initializes the FromFolder class.

        Args:
            folder_path (Path): Path to the folder containing files.
            keep_extensions (List[str], optional): A list of file extensions to include (e.g., ['.txt', '.py']).
            ignore_extensions (List[str], optional): A list of file extensions to exclude (e.g., ['.log', '.tmp']).

        Raises:
            ValueError: If both `keep_extensions` and `ignore_extensions` are provided.
        """
        self.folder_path = folder_path
        self.keep_extensions = keep_extensions or []
        self.ignore_extensions = ignore_extensions or []

        # Validate that both lists are not simultaneously set
        if self.keep_extensions and self.ignore_extensions:
            raise ValueError("You can specify either keep_extensions or ignore_extensions, but not both.")

    def stream(self) -> Iterable["LineInfo"]:
        """
        Streams LineInfo objects from files in the folder, filtering based on file extensions.

        Yields:
            LineInfo: LineInfo objects from each filtered file in the folder.
        """
        for filepath in self.folder_path.iterdir():
            if filepath.is_file() and self._should_include(filepath.suffix):
                yield from FromFile(filepath).stream()

    def _should_include(self, extension: str) -> bool:
        """
        Determines if a file should be included based on the keep/ignore lists.

        Args:
            extension (str): The extension of the file (e.g., '.txt', '.py').

        Returns:
            bool: True if the file should be included, False otherwise.
        """
        extension = extension.lower()
        if self.keep_extensions:
            return extension in self.keep_extensions
        if self.ignore_extensions:
            return extension not in self.ignore_extensions
        return True  # If neither list is specified, include all files.


class FromRGlob:
    def __init__(
            self,
            folder_path: Path,
            keep_extensions: List[str] | None = None,
            ignore_extensions: List[str] | None = None,
            ignore_folders: List[str] | None = None,
    ):
        """
        Initializes the FromRGlob class.

        Args:
            folder_path (Path): Path to the root folder to search files recursively.
            keep_extensions (List[str] | None, optional): A list of file extensions to include (e.g., ['.txt', '.py']).
            ignore_extensions (List[str] | None, optional): A list of file extensions to exclude (e.g., ['.log', '.tmp']).
            ignore_folders (List[str] | None, optional): A list of folder names to exclude from search (e.g., ['ignore_me', 'tmp']).

        Raises:
            ValueError: If both `keep_extensions` and `ignore_extensions` are provided.
        """
        self.folder_path = folder_path
        self.keep_extensions = keep_extensions or []
        self.ignore_extensions = ignore_extensions or []
        self.ignore_folders = ignore_folders or []

        # Validate that both lists are not simultaneously set
        if self.keep_extensions and self.ignore_extensions:
            raise ValueError("You can specify either keep_extensions or ignore_extensions, but not both.")

    def stream(self) -> Iterable["LineInfo"]:
        """
        Streams LineInfo objects from files in the folder hierarchy, filtering based on file extensions and folders.

        Yields:
            LineInfo: LineInfo objects from each filtered file in the folder.
        """
        for filepath in self.folder_path.rglob("*"):
            # Skip folders in the ignore_folders list
            if any(ignored in filepath.parts for ignored in self.ignore_folders):
                continue

            # Process files with matching extensions
            if filepath.is_file() and self._should_include(filepath.suffix):
                yield from FromFile(filepath).stream()

    def _should_include(self, extension: str) -> bool:
        """
        Determines if a file should be included based on the keep/ignore lists.

        Args:
            extension (str): The extension of the file (e.g., '.txt', '.py').

        Returns:
            bool: True if the file should be included, False otherwise.
        """
        extension = extension.lower()
        if self.keep_extensions:
            return extension in self.keep_extensions
        if self.ignore_extensions:
            return extension not in self.ignore_extensions
        return True  # If neither list is specified, include all files.


class FromString(InputBase):
    def __init__(self, text: str, separator='\n', name='text'):
        self.name = name
        self.text = text
        self.sep = separator

    def stream(self) -> Iterable[LineInfo]:
        for line_number, data in enumerate(self.text.split(self.sep), start=1):
            yield LineInfo(sequence_id=line_number, resource_name=self.name, data=data)
