"""
This module contains input components for the data pipeline.

The classes in this module provide different ways to stream data into the pipeline, such as:
- `FromFile`: Streams data from a file with various handlers.
- `FromFolder`: Streams data from files in a folder, with options to filter files by patterns.
- `FromGlob`: Streams data by matching file paths using glob patterns.
- `FromString`: Streams data from a string.
- `FromStrings`: Streams data from multiple strings.

These classes extend the base functionality and provide flexibility in defining
different input sources for the pipeline.
"""


import os
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable, Type

from ._base import FileHandlerBase, InputBase
from ._file_handler import TextFileHandler
from ._streamitem import LineStreamItem


class FromFile(InputBase):
    """
    Reads data from a single file using a specified handler.

    Attributes:
        filepath (str): Path to the file to be read.
        _handler (str): The name of the handler used to read the file.
    """
    # Registry mapping extensions to file_handler classes
    _HANDLER_MAP: dict[str, type[FileHandlerBase]] = {}

    def __init__(self, filepath: str, handler: type[FileHandlerBase] = None):
        """
        Initialize a FromFile object with the provided file path and optional handler.

        Args:
            filepath (str): Path to the file to be processed.
            handler (type[FileHandlerBase], optional): Custom file handler class. If not provided,
                the appropriate handler will be selected based on the file extension.
        """
        self.filepath = Path(filepath).resolve()
        self._handler = handler  # Store the custom handler class (if any)
        self._file_handler_instance = None  # Instantiate lazily only when accessed

    def stream(self):
        """
        Stream items from the file by delegating to the appropriate file_handler's `stream` method.

        This method can be used both within and outside of a context manager. If used outside,
        the file handler will be automatically initialized and cleaned up.

        Yields:
            LineStreamItem instances representing each line from the file.
        """
        # Check if the file handler is already initialized (via a context manager)
        handler_initialized = self._file_handler_instance is not None

        if handler_initialized:
            # If already initialized, just stream from the file handler
            yield from self.file_handler.stream()
        else:
            # If not initialized, use a context manager to open it temporarily
            with self.file_handler as temp_handler:
                self._file_handler_instance = temp_handler
                yield from temp_handler.stream()

    def __enter__(self):
        """
        Delegate context setup to the file_handler instance.
        """
        self._file_handler_instance = self.file_handler.__enter__()
        return self._file_handler_instance

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Delegate context teardown to the file_handler instance.
        """
        if self._file_handler_instance:
            self._file_handler_instance.__exit__(exc_type, exc_value, traceback)
            self._file_handler_instance = None


    @property
    def file_handler(self)->FileHandlerBase:
        """
        Lazily resolve and instantiate the appropriate handler for the file.
        """
        if hasattr(self, "_file_handler_instance") and self._file_handler_instance is not None:
            return self._file_handler_instance
        return self._init_handler()

    def _init_handler(self):
        """Initialize a handler by creating one with the bazbaz123
        """
        handler_class = self._get_handler(self.filepath, self._handler)

        # Instantiate the handler with the file path
        self._file_handler_instance = handler_class(self.filepath)
        return self._file_handler_instance

    def _get_handler(self,
                     filepath: Path,
                     handler: type[FileHandlerBase] | None) -> type[FileHandlerBase]:
        """
        Choose the appropriate file_handler, either custom or from the registry.

        Args:
            filepath (Path): The path to the file.
            handler (type[FileHandlerBase] | None): A custom file_handler class.

        Returns:
            type[FileHandlerBase]: The chosen file handler class.
        """
        # If a custom file_handler is provided, use it
        if handler is not None:
            return handler

        # Otherwise, look up the file_handler in the registry
        extension = filepath.suffix.lower()
        handler_class = self._HANDLER_MAP.get(extension)

        # Return the matched handler or default to TextFileHandler
        if handler_class is not None:
            return handler_class

        # Default to TextFileHandler if no match is found in HANDLER_MAP
        from ._file_handler import TextFileHandler
        return TextFileHandler

    @classmethod
    def clear_registered_handlers(cls):
        """
        Clears all registered file handlers from the registry.

        This is useful for testing purposes to reset the state of the registry.
        """
        cls._HANDLER_MAP.clear()

    @classmethod
    def register_handler(cls, extension_pattern: str, force: bool = False):
        """
        Register a handler for a specific file extension pattern (e.g., "*.log").

        Works as a decorator when used with `@FromFile.register_handler(".log")`.

        Args:
            extension_pattern (str): The file extension pattern (e.g., ".log").
            force (bool): If True, overwrite an existing handler.

        Returns:
            A decorator function for registering a handler class.

        Raises:
            ValueError: If attempting to register an already existing handler (and `force=False`).
        """
        # Validate and normalize the extension pattern
        if not extension_pattern.startswith("."):
            msg = f"Invalid extension pattern '{extension_pattern}'. Must start with '.'."
            raise ValueError(msg)

        extension = extension_pattern.lower()

        # Define the decorator function
        def decorator(handler_class):
            # Handle existing registrations
            if extension in cls._HANDLER_MAP:
                if not force:
                    raise ValueError(
                        f"Handler for extension '{extension}' is already registered. "
                        f"Use force=True to overwrite."
                    )

                print(f"Overwriting existing handler for extension '{extension}'...")

            # Register or overwrite the handler
            cls._HANDLER_MAP[extension] = handler_class
            return handler_class  # Return the handler class for chaining

        return decorator  # Return the decorator function

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
        folder_path (Path): Path to the folder containing files to be processed.
        file_handler (Type[FileHandlerBase]): The file handler class used to process files.
                                              Defaults to `TextFileHandler` if not specified.
        keep_patterns (list[str]): Inclusion patterns for filtering (e.g., ['*.txt', '*.py']).
        ignore_patterns (list[str]): Exclusion patterns for filtering (e.g., ['*.log', '*.tmp']).

    Raises:
        ValueError: If both `keep_patterns` and `ignore_patterns` are provided simultaneously.
    """

    def __init__(
            self,
            folder_path: Path,
            file_handler: Type[FileHandlerBase] | None = None,
            keep_patterns: list[str] | None = None,
            ignore_patterns: list[str] | None = None,
    ):
        """
        Initializes the `FromFolder` instance.

        This method sets up the folder path, file handler, and optional file filtering
        patterns for processing files in a directory. If no file handler is explicitly
        provided, the default `TextFileHandler` is used.

        Args:
            folder_path (Path): Path to the folder containing files to process.
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
        self.folder_path = folder_path
        self.file_handler = file_handler or TextFileHandler
        self.keep_patterns = keep_patterns or []
        self.ignore_patterns = ignore_patterns or []

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

    def _should_include(self, file_path: Path) -> bool:
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
            file_path (Path): The path of the file to evaluate.

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
            raise ValueError(f"Glob folder_path {self.folder_path} does not exist.")

    def __enter__(self):
        """
        Enter the context. No real resource is acquired, but this makes
        FromGlob interchangeable with FromFile in context managers.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context. Nothing needs to be closed since this is really a container
        for code that processes a bunch of files.
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
        """
        Initialize the FromString instance.

        Args:
            name (str): Name of the string source.
            text (str): The string data to stream from.
            sep (str, optional): Separator for splitting string into lines. Defaults to '\n'.
        """
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

    def stream(self) -> Iterable[LineStreamItem]:
        """Stream lines split by the specified separator."""
        for line_number, data in enumerate(self.text.split(self.sep), start=1):
            yield LineStreamItem(sequence_id=line_number, resource_name=self.name, data=data)


class FromStrings(InputBase):
    """
    A class to handle the streaming of multiple input strings, splitting each string
    into smaller chunks based on a specified separator, and yielding them as
    `LineStreamItem` objects.

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

    def stream(self) -> Iterable[LineStreamItem]:
        """
        Stream the processed strings as `LineStreamItem` objects.

        This method iterates over the input strings, splitting each string into smaller chunks
        based on the provided separator. Each chunk is yielded as an instance of `LineStreamItem`.
        The `sequence_id` of the `LineStreamItem` corresponds to the chunk's position in the
        original string, and the `resource_name` is augmented with an identifier for each
        input string.

        Yields:
            LineStreamItem: An item representing a single chunk of text and associated metadata.

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
        for id_, line in enumerate(self.lines, start=1):
            # Use the generator returned by FromString to handle separation and streaming
            from_string = FromString(line, separator=self.sep, name=f"{self.name}-{id_}").stream()
            yield from from_string
