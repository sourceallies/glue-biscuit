from typing import List, Dict
from awsglue import DynamicFrame


class DynamicFrameMatcher(DynamicFrame):
    """
    A Pytest Matcher implementation that can compare the provided List[Dict] to a Glue DynamicFrame
    """
    expected = []

    def __init__(self, expected: List[Dict]):
        self.expected = expected

    def __repr__(self):
        return f"Expected: {repr(self.expected)}"

    def __eq__(self, other: DynamicFrame):
        other_rows = [row.asDict() for row in other.toDF().collect()]
        return other_rows == self.expected
