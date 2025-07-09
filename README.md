
## Overview
`pipethis` is a Python library designed to simplify the process of building, extending, and executing data pipelines.
It provides a modular framework that enables users to define pipelines as a sequence of operations and data 
transformations. Each pipeline can ingest data, process it through customizable transformations, and output 
the results in various formats. The library follows a clean and extensible architecture, allowing developers to
integrate predefined components or implement their own. Whether you're processing text files, building ETL pipelines, 
or experimenting with streaming data, `pipethis` streamlines the process, making it easy to assemble 
pipelines programmatically.


### **1. Inputs**
Inputs determine how data is ingested into the pipeline. The package provides several options, including:
- `FromString`: Reads data from a Python string.
- `FromFile`: Reads data from a file.
- `FromFolder`: Reads data from multiple files in a directory.

Example:
```python
from pipethis import FromString

input_component = FromString("example\ntext")
```

### **2. Transforms**
Transforms define how to process or manipulate data line by line. Examples include:
- `UpperCase`: Converts each line to uppercase.
- `AddMetaData`: Adds metadata like line numbers or source details.
- `RegexSkipFilter`: Excludes lines matching a regex pattern.

Example:
```python
from pipethis import UpperCase, RegexSkipFilter

upper = UpperCase()
filter_lines = RegexSkipFilter("skip_this")
```

### **3. Outputs**
Outputs control where the processed data is sent. Some common options are:
- `ToStdOut`: Prints to the console.
- `ToFile`: Writes output to a file.
- `ToString`: Aggregates processed data as a single string.

Example:
```python
from pipethis import ToString

output_component = ToString()
```

---

## Writing Custom Components

Extend the framework by creating custom Input, Transform, or Output components. All custom components must inherit from their respective base classes.

### Custom Input Example
```python
from pipethis import InputBase
from pipethis import LineStreamItem

class CustomInput(InputBase):
    def stream(self):
        yield LineStreamItem(1, "custom_source", "Hello, custom input!")
```

### Custom Transformation Example
```python
from pipethis import TransformBase

class ReplaceStrings(TransformBase):
    def transform(self, line):
        # Replace "old" with "new" in the content
        line.content = line.content.replace("old", "new")
        return line
```

### Custom Output Example
```python
from pipethis import OutputBase

class LogToFile(OutputBase):
    def write(self, line):
        with open("log.txt", "a") as log_file:
            log_file.write(line.content + "\n")
```

---

## Advanced Usage

### Pipeline Execution
Pipelines can be built incrementally and executed using the `run()` method:
```python
from pipethis import Pipeline, FromString, UpperCase, ToFile

pipeline = Pipeline() | FromString("data pipeline\nexample code") | UpperCase() | ToFile("output.txt")
pipeline.run()
```

### Error Handling
Use `try/except` blocks to handle exceptions during pipeline execution:
```python
from pipethis import Pipeline, FromString, UpperCase, ToFile
try:
   pipeline = Pipeline() | FromString("data pipeline\nexample code") | UpperCase() | ToFile("output.txt")
   pipeline.run()
except IOError as iox:
   print(f"Unexpected error {iox}")
```

---

## Development and Testing

To test or modify the package locally:

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install in editable mode:
   ```bash
   pip install -e .
   ```

3. Run tests using `pytest`:
   ```bash
   pytest
   ```

---
## Testing
The current 

---
## Contributions

Contributions are welcome! Please fork the repository, create a new branch for your changes, and submit a pull request. Ensure all tests pass before submitting.

---

## License

This project is distributed under the MIT License. See the `LICENSE` file for full details.

---

## Questions or Feedback?

If you have questions or suggestions, please open an issue on GitHub. Contributions are welcome!