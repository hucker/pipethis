import json
import pathlib
import datetime as dt

from ._base import OutputBase
from ._streamitem import LineStreamItem

class ToJson(OutputBase):
    """
    A class to handle output directed to a file.

    This class writes output to a file specified by the user, with customizable
    file mode and encoding.
    """

    def __init__(self,
                 file_name:str|pathlib.Path,
                 mode:str='w',
                 encoding:str="utf-8",
                 description:str|None=None,
                 run_date:dt.datetime|str|None=None):
        """
        Initializes a class for managing JSON data with attributes for file handling and metadata.
        This class is designed to handle JSON files by opening them in a specified mode and encoding
        while maintaining metadata such as a description and run date.

        Args:
            file_name (str, optional): The name of the file to which output will be written.
                Defaults to None, in which case output is directed to stdout.
            mode (str, optional): The mode in which the file is opened. Defaults to 'w'.
            encoding (str, optional): The file encoding. Defaults to "utf-8".
            description (str, optional): A description of the data. Defaults to None.
            run_date (str, optional): The date and time the data was processed. Defaults to None.

        """
        super().__init__()
        self.file_name = str(file_name)
        self.mode = mode
        self.encoding = encoding
        self.file = None  # File will be opened in context
        self.description = description or "JSON Data"
        self.records = []

        if run_date is None:
            self.run_date = dt.datetime.now().isoformat()
        elif isinstance(run_date, str):
            self.run_date = run_date    # ISO Format required
        elif isinstance(run_date, dt.datetime):
            self.run_date = run_date.isoformat()
        else:
            raise ValueError("Invalid run_date type. Must be str or datetime.datetime.")
        self.json_data = {}

    def __enter__(self):
        """Open the file and return the instance."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Ensure the file is closed properly on exit. """
        self.close()

    def write(self, lineinfo: LineStreamItem):
        """
        Write the output of a LineInfo object to a file.

        Args:
            lineinfo (LineStreamItem): The LineInfo object to write.
        """
        self.records.append({'sequence_id':lineinfo.sequence_id,
                             'resource_name':lineinfo.resource_name,
                             'data':lineinfo.data})


    def close(self):
        """Save the records to a json file."""
        self.json_data = {'header':{'description':self.description,
                       'date':self.run_date,
                       'count':len(self.records)},
             'records':self.records}
        """Ensure the file is closed properly on exit."""
        with open(self.file_name, mode=self.mode, encoding=self.encoding) as outfile:
            json.dump(self.json_data, outfile)

