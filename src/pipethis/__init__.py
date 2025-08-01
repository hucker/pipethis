"""
pipethis: A Python package for creating data transformation pipelines.

This module provides various components, including inputs, outputs, file handlers,
and transforms, that allow users to build flexible and reusable pipelines for
processing streaming data.

Exports:
    - Base classes
    - Input components
    - Output components
    - Transform components
    - Line data representation
    - Pipeline class
"""
# Exporting base classes
# Can be overridden by client.
from ._base import FileHandlerBase, InputBase, OutputBase, StreamItem, TransformBase
from ._file_handler import TextFileHandler
# Exporting input components
from ._inputs import FromFile, FromFolder, FromGlob, FromString, FromStrings
# Exporting output components
from ._output import ToFile, ToStdOut, ToString
# Exporting pipeline class
from ._pipeline import Pipeline
# Exporting line data representation
from ._streamitem import LineStreamItem
# Exporting transform components
from ._transform import (AddMetaData, LowerCase, PassThrough, RegexKeepFilter, RegexSkipFilter,
                         RegexSubstituteTransform, SkipRepeatedBlankLines, UpperCase)

# Define the public API of the package
__all__ = [
    # Base classes
    "StreamItem", "InputBase", "TransformBase", "OutputBase",

    # Inputs
    "FromFile", "FromFolder", "FromGlob", "FromString", "FromStrings",

    # Line data representation
    "LineStreamItem",

    # File handlers
    "FileHandlerBase",
    "TextFileHandler",

    # Outputs
    "ToStdOut", "ToFile", "ToString",

    # Pipeline
    "Pipeline",

    # String Transforms
    "PassThrough", "UpperCase", "LowerCase", "RegexKeepFilter",
    "AddMetaData", "RegexSkipFilter", "RegexSubstituteTransform",
    "SkipRepeatedBlankLines"
]
