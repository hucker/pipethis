# Exporting base classes
from ._base import DataInfo, InputBase, TransformBase, OutputBase

# Exporting input components
from ._inputs import FromFile, FromFolder, FromRGlob, FromString

# Exporting line data representation
from ._lineinfo import LineInfo

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
    RegexSubstituteTransform,
    SkipRepeatedBlankLines
)

# Define the public API of the package
__all__ = [
    # Base classes
    "DataInfo", "InputBase", "TransformBase", "OutputBase",

    # Inputs
    "FromFile", "FromFolder", "FromRGlob", "FromString",

    # Line data representation
    "LineInfo",

    # Outputs
    "ToStdOut", "ToFile", "ToString",

    # Pipeline
    "Pipeline",

    # Transforms
    "PassThrough", "UpperCase", "LowerCase",
    "AddMetaData", "RegexSkipFilter", "RegexSubstituteTransform",
    "SkipRepeatedBlankLines"
]
