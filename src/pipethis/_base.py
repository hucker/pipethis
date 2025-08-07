"""
This module provides base classes for the `pipethis` package.

The classes defined here are abstract base classes (ABC) used for core
components, including file handlers, input, output, and transformation
elements in the pipeline. These classes serve as the foundation for extending
and implementing custom functionality in pipelines.
"""

import dataclasses
import pathlib
from abc import ABC, abstractmethod
from typing import Any, Iterable

from ._logging import get_logger

# Create local logger
logger = get_logger(__name__)


@dataclasses.dataclass
class StreamItem(ABC):
    """
    Represents an item of data streaming through the pipeline.

    The `StreamItem` contains essential properties, such as its sequence ID and
    associated resource name, enabling it to hold metadata and stream data.

    Attributes:
        sequence_id (int): Sequential identifier for the data (e.g., line #, row #, page #).
        resource_name (str): Name of the data source (e.g., file name, table, or sheet name).
        data (Any): The actual data content being processed.
    """

    sequence_id: int
    resource_name: str | pathlib.Path
    data: Any

    def __post_init__(self):
        """
        Perform additional validation and cleanup on initialization.

        Ensures that sequence_id is positive and that resource_name is a non-empty string
        allowing for pathlib objects to easily handled.
        """

        # If resource_name is a Path, extract only the file name and extension
        if isinstance(self.resource_name, pathlib.Path):
            self.resource_name = self.resource_name.name

        # Avoid confusion and don't allow bad whitespace
        if isinstance(self.resource_name, str):
            self.resource_name = self.resource_name.strip()

        # Validate `resource_name` (must be a non-empty string)
        if not isinstance(self.resource_name, str) or not self.resource_name:
            raise ValueError("Resource_name must be a non-empty string")

        # Validate `sequence_id` (must be a positive integer)
        if not isinstance(self.sequence_id, int) or self.sequence_id <= 0:
            raise ValueError("Sequence_id must be a positive integer")

        # Abstract validation for subclasses
        self.validate()

    @abstractmethod
    def validate(self):
        """
        Perform additional subclass-specific validation.

        This method must be implemented by subclasses to add custom validation logic.
        """
        raise NotImplementedError("Subclasses must implement 'validate'")  # pragma: no cover


class FileHandlerBase(ABC):
    """
    Base class for handling files in the pipeline.

    This abstract class provides a foundation for extending file reading
    functionality and ensures resource management through context manager support.
    """

    def __init__(self, file_path: pathlib.Path):
        """
        Initialize the file handler with the file path.

        Args:
            file_path (pathlib.Path): Path to the file to be handled.
        """
        self.file_path = file_path

    @abstractmethod
    def stream(self) -> Iterable[StreamItem]:
        """
        Stream the content of the file as StreamItems.

        Returns:
            Iterable[StreamItem]: An iterable of `StreamItem` objects.
        """
        raise NotImplementedError("Subclasses must implement 'stream'")  # pragma: no cover

    def __enter__(self):
        """
        Prepare resources for processing the file.

        Returns:
            FileHandlerBase: The instance of the file handler.
        """
        return self  # pragma: no cover

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Cleanup resources when exiting context.

        Args:
            exc_type (type): Exception type raised during the context.
            exc_value (Exception): Exception value raised during the context.
            traceback (Traceback): Traceback object for the exception.
        """


class InputBase(ABC):
    """
    Base class for all input components in the pipeline.

    Input components are responsible for streaming data into the pipeline.
    """

    def _list_or_string(self, items: list[str] | str | None, sep: str = " ") -> list[str]:
        """
        Converts the input into a list of strings.

        If the input is `None`, an empty list is returned. If the input is a string, it is split
        into a list of strings using space as delimiter. If the input is a list of strings,
        it is returned as-is. If the input contains elements that are not strings, or if the
        input type is invalid, a `ValueError` is raised.

        None is converted to []
        "" is converted to []
        "foo" is converted to ["foo"]
        "foo bar" is converted to ["foo", "bar"]
        ["foo", "bar"] is returned as-is

        This method is used by __init__ to allow flexible input handling for parameters that
        can be a list of strings.

        Args:
            items (list[str] | str | None): Input value to be converted. It can be a string,
                a list of strings, or `None`.
            sep (str, optional): Delimiter to use for splitting strings. Defaults to " ".

        Returns:
            list[str]: A list of strings derived from the input.

        Raises:
            ValueError: If the input is not a string, a list of strings, or `None`, or if the
                list contains elements that are not strings.
       """
        if items is None or items == '':
            return []
        elif isinstance(items, str):
            return items.split(sep=sep)
        elif isinstance(items, list):
            for item in items:
                if not isinstance(item, str):
                    msg = f"Input must be string or list of strings: `{item}` is a {type(item)}"
                    raise ValueError(msg)
            return items

        raise ValueError("Input must be a string or a list of strings")

    @abstractmethod
    def stream(self) -> Iterable[StreamItem]:
        """
        Stream input data as StreamItems.

        Must be implemented by subclasses to define specific input behavior.

        Returns:
            Iterable[StreamItem]: An iterable StreamItem objects.
        """
        raise NotImplementedError("Subclasses must implement 'stream'")  # pragma: no cover

    def to_list(self):
        """
        Converts the streamed data into a list of StreamItem objects.

        This method is a convenience wrapper for `stream` that returns a list of StreamItem objects.
        and will often be used in testing.

        Returns:
            list: A list of StreamItem objects generated by the `stream` method.
        """
        return list(self.stream())

    def __or__(self, other):
        """
        Overloads the '|' operator for Inputs to support chaining in pipelines.

        Automatically creates a Pipeline instance if necessary and adds this
        InputBase instance as the pipeline's input.
        """
        from ._pipeline import Pipeline

        # Otherwise, create a new pipeline, add this input, and chain to "other"
        new_pipeline = Pipeline()
        new_pipeline.add_input(self)
        return new_pipeline | other


class TransformBase(ABC):
    """
    Base class for all transformation components in the pipeline.

    Transform components operate on `StreamItem` objects, allowing
    modification or enhancement of the data.
    """

    @abstractmethod
    def transform(self, item: StreamItem) -> Iterable[StreamItem]:
        """
        Apply a transformation to the given StreamItem.

        Args:
            item (StreamItem): The StreamItem instance to be transformed.

        Returns:
            Iterable[StreamItem]: An iterable containing transformed StreamItems.
        """
        raise NotImplementedError("Subclasses must implement 'transform'")  # pragma: no cover


class OutputBase(ABC):
    """
    Base class for all output components in the pipeline.

    Output components are responsible for writing or processing the final results
    of the pipeline.
    """

    def __init__(self):
        """
        Initialize the output component. Default behavior does nothing.
        """

    def __enter__(self):
        """
        Prepare resources for output.

        Default context manager behavior, which does nothing.
        Returns:
            OutputBase: The instance of the output component.
        """
        return self  # pragma: no cover

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Cleanup resources for output.

        Default context manager behavior, which does nothing.

        Args:
            exc_type (type): Exception type raised during the context.
            exc_value (Exception): Exception value raised during the context.
            traceback (Traceback): Traceback object for the exception.
        """

    @abstractmethod
    def write(self, lineinfo: StreamItem):
        """
        Write the output of a StreamItem.

        Args:
            lineinfo (StreamItem): The StreamItem to be written.
        """
        raise NotImplementedError("Subclasses must implement 'write'")  # pragma: no cover

    def close(self):
        """
        Perform cleanup operations when output operations are finished.

        Default behavior does nothing but can be overridden by subclasses.
        """
