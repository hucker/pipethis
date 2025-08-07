"""
Logging support for Ten8t.

The purpose of this module is to set up package logging in a general way that integrates
with other packages and loggers.

The key feature is that you can call pipethis_setup_logging AFTER you have configured
other packages' loggers' and it will just work along with them.

If the only logging in the system is set up with this tool then you can still
use this and provide a target stream (e.g., stdout) and a file name.

This method is set up to manage a single global logger object named pipethis_logger
that should be used by all the ten8t modules.

"""
import logging
import pathlib

# Global variable for library logger
PIPETHIS_LOGGER = None  # This will be initialized later

DEFAULT_NAME = "pipethis"
DEFAULT_FORMAT_STRING = "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s"


def initialize_pipethis_logger() -> logging.Logger:
    """
    Initialize and reset the 'pipethis' logger with a default NullHandler.
    Ensures the logger is in a clean state and prevents unintended log propagation.

    Returns:
        logging.Logger: A reset logger for the library.
    """
    global PIPETHIS_LOGGER

    # Create or retrieve a logger for the library
    PIPETHIS_LOGGER = logging.getLogger(DEFAULT_NAME)

    # Clear all handlers to reset the logger (note we are iterating over a copy [:])
    for handler in PIPETHIS_LOGGER.handlers[:]:
        PIPETHIS_LOGGER.removeHandler(handler)

    # Restore the defaults for the logger
    PIPETHIS_LOGGER.setLevel(logging.NOTSET)
    PIPETHIS_LOGGER.propagate = True

    # Add a NullHandler to avoid "No handler found" warnings
    PIPETHIS_LOGGER.addHandler(logging.NullHandler())

    return PIPETHIS_LOGGER


def get_logger(name: str) -> logging.Logger:
    """
    Get a properly named logger for use within the `pipethis` package.

    This ensures all loggers are properly structured under the `pipethis` namespace,
    regardless of how they're imported.

    Args:
        name: Typically __name__ from the calling module

    Returns:
        A Logger instance with the proper namespace
    """
    if not isinstance(name, str):
        raise ValueError(f"The provided name '{name}' is not a logger name.")

    # If the name already starts with our default namespace, use it as is
    if name.startswith(f"{DEFAULT_NAME}.") or name == DEFAULT_NAME:
        return logging.getLogger(name)

    # If it's a completely different module, put it under the pipethis namespace
    raise ValueError(f"The provided name '{name}' is not a valid logger name. ")


# Initialize the logger when the module is imported
initialize_pipethis_logger()


def pipethis_setup_logging(
        level: int = logging.WARNING,
        propagate: bool = True,
        file_name: str | None = None,
        name: str = "",
        stream_=None,
        format_string=DEFAULT_FORMAT_STRING,
) -> logging.Logger:
    """
    Configure the 'pipethis' logger (or one of its children) with logging handlers.
    """
    global PIPETHIS_LOGGER

    # Root logger vs. child logger
    if name == "":
        logger = PIPETHIS_LOGGER
    else:
        logger = logging.getLogger(f"{DEFAULT_NAME}.{name}")

    # Set log level for this logger explicitly
    logger.setLevel(level)

    # Set propagation behavior
    logger.propagate = propagate

    # Ensure a formatter exists for all handlers
    formatter = logging.Formatter(format_string)

    # Validate stream_ if provided
    if stream_ is not None:
        if not hasattr(stream_, "write"):
            msg = f"The provided stream '{stream_}' is not a valid writable stream object."
            raise ValueError(msg)
        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
            stream_handler = logging.StreamHandler(stream_)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

    # Validate file_name if provided and ensure the folder exists
    if file_name:
        file_path = pathlib.Path(file_name)  # Convert to a pathlib Path object
        if file_path.parent and not file_path.parent.exists():
            msg = f"The directory '{file_path.parent}' for the file '{file_name}' does not exist."
            raise ValueError(msg)

        if not any(
                isinstance(h, logging.FileHandler) for h in logger.handlers):
            file_handler = logging.FileHandler(file_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)


    return logger
