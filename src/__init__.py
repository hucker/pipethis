# Exporting base classes
from ._base import StreamItem, InputBase, TransformBase, OutputBase

# Exporting input components
from ._inputs import FromFile, FromFolder, FromRGlob, FromString

# Exporting line data representation
from ._streamitem import LineStreamItem

# Exporting output components
from ._output import ToStdOut, ToFile, ToString

# Exporting pipeline class
from ._pipeline import Pipeline

# Exporting transform components
from ._transform import (
    PassThrough,
    UpperCase,
    LowerCase,
    AddMetaData,
    RegexSkipFilter,
    RegexKeepFilter,
    RegexSubstituteTransform,
    SkipRepeatedBlankLines
)

# Define the public API of the package
__all__ = [
    # Base classes
    "StreamItem", "InputBase", "TransformBase", "OutputBase",

    # Inputs
    "FromFile", "FromFolder", "FromRGlob", "FromString",

    # Line data representation
    "LineStreamItem",

    # Outputs
    "ToStdOut", "ToFile", "ToString",

    # Pipeline
    "Pipeline",

    # String Transforms
    "PassThrough", "UpperCase", "LowerCase","RegexKeepFilter",
    "AddMetaData", "RegexSkipFilter", "RegexSubstituteTransform",
    "SkipRepeatedBlankLines"
]
