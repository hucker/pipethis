"""
This module provides the `FromFile` class for reading data from files in a
streaming fashion. It supports customizable file handlers to process different
file formats or types, allowing flexible file input in pipelines.

The core functionality includes loading files, managing file handlers,
and reading line-by-line or file-by-file depending on the handler used.

Example Usage:
    >>> from pipethis._input_from_file import FromFile
    >>> with FromFile("path/to/file.log") as input_file:
    ...     for line in input_file.stream():
    ...         print(line.data)
"""
import pathlib
from ._base import FileHandlerBase, InputBase
from ._logging import get_logger

# Create local logger
logger = get_logger(__name__)


class FromFile(InputBase):
    """
    Reads data from a single file using a specified handler.

    Attributes:
        filepath (str): pathlib.Path to the file to be read.
        _handler (str): The name of the handler used to read the file.
    """
    # Registry mapping extensions to file_handler classes
    _HANDLER_MAP: dict[str, type[FileHandlerBase]] = {}

    def __init__(self,
                 filepath: str | pathlib.Path,
                 handler: type[FileHandlerBase] = None):
        """
        Initialize a FromFile object with the provided file path and optional handler.

        Args:
            filepath (str): Path to the file to be processed.
            handler (type[FileHandlerBase], optional): Custom file handler class. If not provided,
                the appropriate handler will be selected based on the file extension.
        """
        logger.debug("Init FromFile with path: %s", filepath)
        self.filepath = pathlib.Path(filepath).resolve()
        self._handler = handler  # Store the custom handler class (if any)
        self._file_handler_instance = None  # Instantiate lazily only when accessed

    def stream(self):
        """
        Stream items from the file by delegating to the appropriate file_handler's `stream` method.

        This method can be used both within and outside a context manager. If used outside,
        the file handler will be automatically initialized and cleaned up.

        Yields:
            LineStreamItem instances representing each line from the file.
        """
        logger.info("Streaming content from the file: %s", self.filepath)

        # Check if the file handler is already initialized (via a context manager)
        handler_initialized = self._file_handler_instance is not None

        if handler_initialized:
            # If already initialized, just stream from the file handler
            yield from self.file_handler.stream()  # pragma no cover
        else:
            # If not initialized, use a context manager to open it temporarily
            with self.file_handler as temp_handler:
                self._file_handler_instance = temp_handler
                yield from temp_handler.stream()

    def __enter__(self):
        """
        Delegate context setup to the file_handler instance.
        """
        logger.debug("Entering context for file: %s", self.filepath)
        self._file_handler_instance = self.file_handler.__enter__()
        return self._file_handler_instance

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Delegate context teardown to the file_handler instance.
        """
        logger.debug("Exiting context for file: %s", self.filepath)
        if self._file_handler_instance:
            self._file_handler_instance.__exit__(exc_type, exc_value, traceback)
            self._file_handler_instance = None

    @property
    def file_handler(self) -> FileHandlerBase:
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
                     filepath: pathlib.Path,
                     handler: type[FileHandlerBase] | None) -> type[FileHandlerBase]:
        """
        Choose the appropriate file_handler, either custom or from the registry.

        Args:
            filepath (pathlib.Path): The path to the file.
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
        logger.info("Clearing all registered file handlers.")
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
