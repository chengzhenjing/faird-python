from dataframe import DataFrame
from parser import csv_parser
import os


def open(dataframe_id: str) -> DataFrame:
    """
    Open the specified dataframe and return a DataFrame object.

    Args:
        dataframe_id (str): The unique identifier of the dataframe.
    Returns:
        DataFrame: A DataFrame object containing the parsed data.
    """

    # Determine the file extension
    file_extension = os.path.splitext(dataframe_id)[1].lower()

    # Use a dictionary to simulate a switch case for parser selection
    parser_switch = {
        ".csv": csv_parser.CSVParser,
        ".json": None,
        ".xml": None,
    }

    # Get the corresponding parser class
    parser_class = parser_switch.get(file_extension)
    if not parser_class:
        raise ValueError(f"Unsupported file extension: {file_extension}")

    # Instantiate the parser and parse the file
    parser = parser_class()
    arrow_table = parser.parse(dataframe_id)
    return DataFrame(id=dataframe_id, data=arrow_table)
