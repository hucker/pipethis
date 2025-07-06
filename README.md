# ipeline Framework
This project provides a modular and extensible framework for building **data processing pipelines**. It allows you to read input from various sources, apply a series of transformations, and write the output to different destinations.
The framework is designed with flexibility and scalability in mind, using an **Input → Transform → Output** model, where you can mix and match components to fit your specific use case.
## Key Features
- **Modular Input, Transformation, and Output Components**: Build pipelines using a clear and logical data flow model.
- **Custom Transformations**: Extend functionality easily by implementing new transformations.
- **Operator-Based Syntax**: Use a clean, chainable `|` pipeline definition for concise and readable code.

## Folder and File Structure
- **`_inputs.py` **: Contains implementations of various input sources (e.g., files, folders, text strings).
- **`_transform.py` **: Defines transformations (e.g., uppercase text, regex filtering, etc.) that can be applied to lines of text.
- **`_output.py` **: Includes output components to write data (e.g., to stdout, files, or strings).
- **`_pipeline.py` **: Implements the pipeline logic to connect inputs, transformations, and outputs for execution.
- **`_lineinfo.py` **: A helper file defining the `LineInfo` data class to encapsulate metadata about lines in the pipeline.
- **`_base.py` **: Abstract base classes for Input, Transform, and Output components to ensure consistency and extendability.
- **`__init__.py` **: Reserved for initializing the package (empty for now).

## Installation
Clone this repository and ensure you have Python 3.9+ installed. No additional dependencies are required to run this framework.
``` bash
git clone <repository-url>
cd <repository-folder>
```
## Usage
Below is an example of how to define and run a pipeline.
### Example 1: Pipeline with Operator Syntax (`|`)
``` python
from _pipeline import Pipeline
from _inputs import FromString
from _transform import UpperCase
from _output import ToStdOut

pipeline = Pipeline() | FromString("hello\nworld") | UpperCase() | ToStdOut()
pipeline.run()
```
**Output:**
``` 
HELLO
WORLD
```
### Example 2: Explicit Additions
You can also explicitly add pipeline components:
``` python
from _pipeline import Pipeline
from _inputs import FromFile
from _transform import AddMetaData, RegexSkipFilter
from _output import ToFile

pipeline = Pipeline()
pipeline.add_input(FromFile("input.txt"))
pipeline.add_transform(AddMetaData())
pipeline.add_transform(RegexSkipFilter("skip this"))
pipeline.add_output(ToFile("output.txt"))

pipeline.run()
```
## Core Concepts
### 1. **Inputs**
The input stage defines where the data comes from. Examples:
- **`FromFile` **: Reads text from a file.
- **`FromFolder` **: Reads text from all files in a folder.
- **`FromString` **: Reads text provided as a string.

All input components must inherit from `InputBase`.
### 2. **Transforms**
Transforms allow you to process and manipulate each line of input.
Examples:
- **`PassThrough` **: Outputs the line as it is.
- **`UpperCase` **: Converts the line to uppercase.
- **`RegexSkipFilter` **: Skips lines matching a specific regular expression.
- **`AddMetaData` **: Injects metadata (e.g., filename, line number) into the line content.

All transforms must inherit from `TransformBase`.
### 3. **Outputs**
Outputs define where the processed data will be sent.
Examples:
- **`ToStdOut` **: Writes output to standard output (console).
- **`ToFile` **: Writes output to a file.
- **`ToString` **: Gathers output as a single concatenated string.

All outputs must inherit from `OutputBase`.
## Writing Custom Components
### Custom Input
To create a custom input component, inherit from `InputBase` and implement the `stream` method.
``` python
from _base import InputBase
from _lineinfo import LineInfo


class CustomInput(InputBase):
    def stream(self):
        yield LineInfo(1, "custom_source", "Hello from CustomInput")
```
### Custom Transform
To create a custom transform, inherit from `TransformBase` and implement the `transform` method.
``` python
from _base import TransformBase
from _lineinfo import LineInfo


class CustomTransform(TransformBase):
    def transform(self, lineinfo: LineInfo):
        lineinfo.line = f"Processed: {lineinfo.line}"
        yield lineinfo
```
### Custom Output
To create a custom output component, inherit from `OutputBase` and implement the `write` method.
``` python
from _base import OutputBase
from _lineinfo import LineInfo


class CustomOutput(OutputBase):
    def write(self, lineinfo: LineInfo):
        print(f"Custom Output: {lineinfo.line}")
```
## Pipeline Execution
### Using `|` Operator for Chaining
- Combine Input, Transform, and Output components using the pipe (`|`) operator for a readable, chainable syntax.
``` python
pipeline = Pipeline() | InputComponent() | TransformComponent() | OutputComponent()
```
### Explicit Method Usage
- Alternately, you can use explicit methods (`add_input`, `add_transform`, and `add_output`):
``` python
pipeline.add_input(InputComponent())
pipeline.add_transform(TransformComponent())
pipeline.add_output(OutputComponent())
```
## Error Handling
The framework enforces strict ordering of pipeline stages:
1. Input must come first.
2. Transforms can only follow inputs or other transforms.
3. Outputs must follow transforms.

If any invalid stage order is encountered, a `TypeError` is raised.
**Example:**
``` python
from _pipeline import Pipeline
from _inputs import FromString
from _output import ToStdOut

try:
    pipeline = Pipeline() | ToStdOut() | FromString("hello")
except TypeError as e:
    print(f"Pipeline construction error: {e}")
```
## Contributing
To contribute:
1. Fork the repository.
2. Create a new branch (`feature/your-feature`).
3. Commit your changes.
4. Submit a pull request.

## License
This project is distributed under the **MIT License**.
## Related Projects
- Python's `itertools` for data streams and transformations.
- Libraries like Apache Beam and Pandas for data pipelines.

Let me know if you'd like further tweaks or have additional details to include! 😊
