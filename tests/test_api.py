import pytest
from unittest.mock import MagicMock
from app.utils.build_queries import build_unique_query


@pytest.fixture
def mock_table():
    mock_table = MagicMock()
    mock_table.table_id = "your_project.your_dataset.your_table"
    return mock_table

def test_build_unique_query_basic(mock_table):
    columns = ["column1", "column3"]
    expected_query = """
    SELECT
    column1, column3
    FROM `your_project.your_dataset.your_table` AS table
    GROUP BY column1, column3
    HAVING COUNT(*) > 1;
    """
    result = build_unique_query(mock_table, columns)
    assert result == expected_query

def test_build_unique_query_empty_columns(mock_table):
    columns = []
    # Decide on the desired behavior in this case
    # Option 1: Raise an exception
    with pytest.raises(ValueError):
        build_unique_query(mock_table, columns)
