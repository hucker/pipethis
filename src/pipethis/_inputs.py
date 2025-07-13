import os
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable, Type

from ._base import FileHandlerBase, InputBase
from ._file_handler import TextFileHandler
from ._streamitem import LineStreamItem


class FromFile(InputBase):
    # Registry mapping extensions to file_handler classes
    _HANDLER_MAP: dict[str, type[FileHandlerBase]] = {}

    def __init__(self, filepath: str, file_handler: type[FileHandlerBase] | None = None):
        """
        Initialize the FromFile input.

        Args:
            filepath (str): The path to the file to process.
            file_handler (type[FileHandlerBase] | None): Custom file_handler to process the file.
        """
        self.filepath = Path(filepath)
        self.file_handler = self._get_handler(self.filepath, file_handler)

        # Default to text file_handler
        if self.file_handler is None:
            self.file_handler = TextFileHandler

    def stream(self):
        """
        Generate a stream of `StreamItem`s from the file by delegating
        the functionality to the appropriate file file_handler.
        """

        # Instantiate the file_handler with the file path
        handler_instance = self.file_handler(self.filepath)

        # Use `yield from` to directly delegate to the file_handler's stream
        yield from handler_instance.stream()

    def _get_handler(self, filepath: Path, handler: type[FileHandlerBase] | None) -> FileHandlerBase:
        """
        Choose the appropriate file_handler, either custom or from the registry.

        Args:
            filepath (Path): The path to the file.
            handler (type[FileHandlerBase] | None): A custom file_handler class.

        Returns:
            FileHandlerBase: An instance of the chosen file file_handler.
        """

        # If a custom file_handler is provided, use it
        if handler is not None:
            return handler(filepath)

        # Otherwise, look up the file_handler in the registry
        extension = filepath.suffix.lower()
        handler_class = self._HANDLER_MAP.get(extension)

        if handler_class:
            return handler_class(filepath)

        # Default to TextFileHandler if no match is found in HANDLER_MAP
        from ._file_handler import TextFileHandler
        return TextFileHandler(filepath)

    @classmethod
    def clear_registered_handlers(cls):
        """
        Clears all registered file handlers from the registry.

        This is useful for testing purposes to reset the state of the registry.
        """
        cls._HANDLER_MAP.clear()

    @classmethod
    def register_handler(cls, extension: str):
        """
        Register a file_handler class for a specific file extension.

        This is a class-level method that returns a decorator.
        When the returned decorator is used on a file_handler class, it automatically
        registers the class in the file_handler map (`_HANDLER_MAP`) under the specified file extension.

        Args:
            extension (str): File extension (e.g., '.txt', '.jpg', '.log').

        Returns:
            decorator: A decorator function to register file_handler classes.

        Example:
            To register a file_handler for `.log` files, including proper context management:

                import pathlib
                from ._base import FileHandlerBase
                from ._streamitem import LineStreamItem

                @FromFile.register_handler(".log")
                class LogFileHandler(FileHandlerBase):
                    def __init__(self, file_path: pathlib.Path):
                        super().__init__(file_path)
                        self._file = None  # Internal file resource

                    def __enter__(self):
                        self._file = self.file_path.open('r', encoding='utf-8')
                        return self

                    def __exit__(self, exc_type, exc_value, traceback):
                        if self._file:
                            self._file.close()

                    def stream(self):
                        if not self._file:
                            raise RuntimeError(
                                "The file is not open. You must use this file_handler in a context manager."
                            )

                        for sequence_id, line in enumerate(self._file, start=1):
                            if "ERROR" in line:
                                yield LineStreamItem(sequence_id, str(self.file_path), line.strip())

            In this example, the `LogFileHandler` is automatically registered for the
            `.log` file extension. When processing `.log` files, the `LogFileHandler`
            ensures proper resource management using the context handlers `__enter__`
            and `__exit__`.
        """

        extension = extension.lower()

        def decorator(handler_cls: type[FileHandlerBase]):
            cls._HANDLER_MAP[extension] = handler_cls
            return handler_cls

        return decorator

    @classmethod
    def get_registered_handler(cls, extension: str) -> type[FileHandlerBase] | None:
        """
        Retrieve the file_handler class for a given file extension.

        Args:
            extension (str): The file extension to check.

        Returns:
            type[FileHandlerBase] | None: The file_handler class if registered, else None.
        """
        return cls._HANDLER_MAP.get(extension.lower())

    @classmethod
    def get_all_registered_handlers(cls) -> dict[str, type[FileHandlerBase]]:
        """
        Retrieve all registered file handlers.

        Returns:
            dict[str, type[FileHandlerBase]]: A map of file extensions to file_handler classes.
        """
        return cls._HANDLER_MAP


class FromFolder:
    def __init__(
            self,
            folder_path: Path,
            file_handler: Type[FileHandlerBase] | None = None,
            keep_patterns: list[str] | None = None,
            ignore_patterns: list[str] | None = None,
    ):
        """
        Initializes the FromFolder class. The default file_handler is the text file file_handler.

        Args:
            folder_path (Path): Path to the folder containing files.
            file_handler (Type[FileHandlerBase] | None): A custom file_handler class to process all files.
                                                   If None, the file_handler is determined via FromFile's registry.
            keep_patterns (list[str] | None): A list of file extensions to include (e.g., ['.txt', '.py']).
            ignore_patterns (list[str] | None): A list of file extensions to exclude (e.g., ['.log', '.tmp']).

        Raises:
            ValueError: If both `keep_patterns` and `ignore_patterns` are provided.
        """
        self.folder_path = folder_path
        self.file_handler = file_handler or TextFileHandler
        self.keep_patterns = keep_patterns or []
        self.ignore_patterns = ignore_patterns or []

        # Validate that both lists are not simultaneously set
        if self.keep_patterns and self.ignore_patterns:
            raise ValueError("You can specify either keep_patterns or ignore_patterns, but not both.")

    def __enter__(self):
        """
        Enter the context. No real resource is acquired, but this makes
        FromString interchangeable with FromFile in context managers.
        """

        # Use the file_handler to process the file
        if self.file_handler is None:
            raise ValueError("No file file_handler provided for FromFolder.")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context. Nothing needs to be closed since this is really a container
        for code that processes a bunch of files.
        """
        pass

    def stream(self):
        # Iterate through files and dynamically create file handlers
        for file_path in self.folder_path.iterdir():
            if not self._should_include(file_path):
                continue

            # Instantiate the file file_handler for the current file
            with self.file_handler(file_path) as handler:
                # Yield the streamed data
                yield from handler.stream()

    def _should_include(self, file_path):
        # Skip directories
        if file_path.is_dir():
            return False

        # Check keep_patterns (if specified)
        if self.keep_patterns:
            if not any(file_path.match(ext) for ext in self.keep_patterns):
                return False

        # Check ignore_patterns (if specified)
        if self.ignore_patterns:
            if any(file_path.match(ext) for ext in self.ignore_patterns):
                return False

        return True


class FromGlob:
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
        - `?` matches exactly one character (e.g., "file?.txt" matches "file1.txt" but not "file01.txt").
        - `[...]` matches any single character in brackets (e.g., "file[123].txt" matches "file1.txt").
        - Recursive wildcard matching (using `**`) matches files at any depth in the folder hierarchy.

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
            raise ValueError("You can specify either keep_patterns or ignore_patterns, but not both.")

    def __enter__(self):
        """
        Enter the context. No real resource is acquired, but this makes
        FromString interchangeable with FromFile in context managers.
        """

        # Use the file_handler to process the file
        if self.file_handler is None:
            raise ValueError("No file file_handler provided for FromFolder.")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context. Nothing needs to be closed since this is really a container
        for code that processes a bunch of files.
        """
        pass

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
                Path: Paths to files that satisfy the filtering rules, using the specified file handler
                (if provided) to process streamed data.

            Examples:
                1. Include `.txt` files, but ignore `.tmp` files:
                    >>> from_glob = FromGlob(folder_path="/data", keep_patterns=["*.txt"],
                                            ignore_patterns=["*.tmp"])
                    >>> list(from_glob.stream())
                    [Path('/data/file1.txt'), Path('/data/file2.txt')]

                2. Include all files in the folder, but skip logs and temporary files:
                    >>> from_glob = FromGlob(folder_path="/data", ignore_patterns=["*.log", "*.tmp"])
                    >>> list(from_glob.stream())
                    [Path('/data/file1.csv'), Path('/data/file2.json')]

                3. Skip specific subfolders while processing `.csv` files:
                    >>> from_glob = FromGlob(folder_path="/data",
                                            ignore_folders=["archive", "backup"],
                                            keep_patterns=["*.csv"])
                    >>> list(from_glob.stream())
                    [Path('/data/file1.csv'), Path('/data/subdir/file2.csv')]
            """

        for root, dirs, files in os.walk(self.folder_path):
            # Skip ignored folders
            dirs[:] = [d for d in dirs if d not in self.ignore_folders]

            # Iterate through files in the remaining folders
            for file in files:
                file_path = Path(root, file)

                # Apply the matching rules
                if self._should_keep(file_path.name):
                    with self.file_handler(file_path=file_path) as handler:
                        yield from handler.stream()

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


class FromString(InputBase):
    """
    Stream the input string as `LineStreamItem` objects.

    This method splits the provided string into smaller chunks using the specified separator
    and associates each chunk with a sequence ID and a resource name. Each chunk is yielded
    as a `LineStreamItem` object.

    Yields:
        LineStreamItem: An object representing a single chunk of text, with metadata
        such as its sequence ID and the resource name.

    Example:
        >>> text = "line1,line2,line3"
        >>> from_string = FromString(text, separator=",", name="example")
        >>> for item in from_string.stream():
        ...     print(item.sequence_id, item.data)
        1 line1
        2 line2
        3 line3
    """
    def __init__(self, text: str, separator='\n', name='text'):
        self.name = name
        self.text = text
        self.sep = separator

    def __enter__(self):
        """
        Enter the context. No real resource is acquired, but this makes
        FromString interchangeable with FromFile in context managers.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context. Nothing needs to be closed, but this makes
        FromString compatible with context management.
        """
        pass

    def stream(self) -> Iterable[LineStreamItem]:
        """Stream lines split by the specified separator."""
        for line_number, data in enumerate(self.text.split(self.sep), start=1):
            yield LineStreamItem(sequence_id=line_number, resource_name=self.name, data=data)


class FromStrings(InputBase):
    """
    A class to handle the streaming of multiple input strings, splitting each string
    into smaller chunks based on a specified separator, and yielding them as `LineStreamItem` objects.

    This class serves as an abstraction for processing multiple strings or a single string,
    allowing for efficient line-by-line streaming. It also supports context management,
    making it compatible with `with` statements.

    Attributes:
        name (str): A name identifier for the resource, included in each `LineStreamItem`.
        lines (list[str]): A list of input strings to process. If a single string is provided,
            it will be converted into a list containing that string.
        sep (str): The separator used to split each string into smaller chunks. Defaults to '\n'.
    """

    def __init__(self, lines: list[str] | str, separator: str = '\n', name: str = 'text'):
        """
        Initialize the FromStrings instance.

        Args:
            lines (list[str] | str): A list of strings or a single string to be processed.
                                     If a single string is provided, it will be wrapped in a list.
            separator (str, optional): The separator used to split the strings. Defaults to '\n'.
            name (str, optional): A unique name for the resource, used in the `LineStreamItem`.
                                  Defaults to 'text'.
        """
        if isinstance(lines, str):
            lines = [lines]

        self.name = name
        self.lines = lines
        self.sep = separator

    def __enter__(self):
        """
        Enter the context for the `FromStrings` instance.

        This method does not acquire any real resources but makes the `FromStrings`
        class interchangeable with file-based classes like `FromFile` for use in
        context managers. It ensures syntactical compatibility when used with `with` statements.

        Returns:
            FromStrings: The instance of itself.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context for the `FromStrings` instance.

        This method performs no specific operations since there are no open resources
        to close. However, it is implemented to maintain compatibility with context
        management protocols.
        """
        pass

    def stream(self) -> Iterable[LineStreamItem]:
        """
        Stream the processed strings as `LineStreamItem` objects.

        This method iterates over the input strings, splitting each string into smaller chunks
        based on the provided separator. Each chunk is yielded as an instance of `LineStreamItem`.
        The `sequence_id` of the `LineStreamItem` corresponds to the chunk's position in the
        original string, and the `resource_name` is augmented with an identifier for each input string.

        Yields:
            LineStreamItem: An item representing a single chunk of text and its associated metadata.

        Example:
            >>> lines = ["This is a test.", "Another example line."]
            >>> from_strings = FromStrings(lines, separator=' ', name='test')
            >>> for item in from_strings.stream():
            ...     print(item.sequence_id, item.data)
            1 This
            2 is
            3 a
            4 test.
            1 Another
            2 example
            3 line.
        """
        for id, line in enumerate(self.lines, start=1):
            # Use the generator returned by FromString to handle separation and streaming
            from_string = FromString(line, separator=self.sep, name=f"{self.name}-{id}").stream()
            yield from from_string


def test_from_strings_context_manager_and_iteration():
    """
    Test using FromStrings as a context manager with iteration over its streamed lines.
    Verify that lines are properly processed and exited without issues.
    """
    lines = ["This is a test.", "Another example line."]
    separator = " "

    with FromStrings(lines, separator=separator, name="test") as fs:
        # Collect all items from the stream
        streamed_results = list(fs.stream())

        # Expected result
        expected_results = [
            LineStreamItem(sequence_id=1, resource_name="test-1", data="This"),
            LineStreamItem(sequence_id=2, resource_name="test-1", data="is"),
            LineStreamItem(sequence_id=3, resource_name="test-1", data="a"),
            LineStreamItem(sequence_id=4, resource_name="test-1", data="test."),
            LineStreamItem(sequence_id=1, resource_name="test-2", data="Another"),
            LineStreamItem(sequence_id=2, resource_name="test-2", data="example"),
            LineStreamItem(sequence_id=3, resource_name="test-2", data="line."),
        ]

        # Verify each LineStreamItem in the streamed results matches the expected ones
        assert len(streamed_results) == len(expected_results)
        for result, expected in zip(streamed_results, expected_results):
            assert result.sequence_id == expected.sequence_id
            assert result.resource_name == expected.resource_name
            assert result.data == expected.data
