from typing import Dict, List
from pyspark.sql import DataFrame


class DataFrameMatcher(DataFrame):
    """
    A PyTest Matcher implementation that can compare the provided List[Dict] to a DataFrame
    """
    expected = []

    def __init__(self, expected: List[Dict]):
        self.expected = expected

    def __repr__(self):
        return f"Expected: {repr(self.expected)}"

    def __eq__(self, other: DataFrame):
        other_rows = [row.asDict() for row in other.collect()]
        return other_rows == self.expected