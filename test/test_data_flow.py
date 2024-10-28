import os
import pytest
import pandas as pd
from unittest.mock import patch, mock_open, Mock
from src.data_flow.main import extract_items, transform_data, load_data_to_csv

mock_extract_page1 = {
    "items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}],
    "pages": 2
}

mock_extract_page2 = {
    "items": [{"id": 3, "name": "item3"}, {"id": 4, "name": "item4"}],
    "pages": 2
}

mock_transform = [
    {"user_id": 77206, "items": [76995, 79174, 70046, 244, 12480], "created_at": "2023-06-04T06:10:28",
     "updated_at": "2024-05-15T07:04:43"}
]

@pytest.fixture
def raw_data():
    return [
        {
            "id": 1,
            "name": "Track1",
            "artist": "Artist1",
            "songwriters": "Songwriter1",
            "duration": "03:45",
            "genres": "Genre1",
            "album": "Album1",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-02T12:00:00",
            "date": "2023-01-01"
        },
        {
            "id": 2,
            "name": "Track2",
            "artist": "Artist2",
            "songwriters": "Songwriter2",
            "duration": "04:30",
            "genres": "Genre2",
            "album": "Album2",
            "created_at": "2023-01-02T12:00:00",
            "updated_at": "2023-01-03T12:00:00",
            "date": "2023-01-02"
        },
        {
            "id": 3,
            "name": "Track3",
            "artist": "Artist3",
            "songwriters": "Songwriter3",
            "duration": "03:00",
            "genres": "Genre3",
            "album": "Album3",
            "created_at": "2023-01-03T12:00:00",
            "updated_at": "2023-01-04T12:00:00",
            "date": None  # NaN
        }
    ]


def test_transform_data_load_data_to_csv_called(raw_data):
    filename = 'test_file.csv'
    with patch('builtins.open', new_callable=mock_open):
        with patch('src.data_flow.main.load_data_to_csv') as mock_load_data_to_csv:
            transform_data(raw_data, filename)
            mock_load_data_to_csv.assert_called_once()


def test_transform_data_drop_na(raw_data):
    filename = 'test_file.csv'
    with patch('builtins.open', new_callable=mock_open), \
            patch('src.data_flow.main.load_data_to_csv'):
        df = transform_data(raw_data, filename)
        assert df.isnull().sum().sum() == 0, "NaN values should be dropped"


def test_transform_data_drop_duplicates(raw_data):
    filename = 'test_file.csv'
    raw_data.pop()
    unique_length = len(raw_data)

    raw_data.append(raw_data[0].copy())

    with patch('builtins.open', new_callable=mock_open), \
            patch('src.data_flow.main.load_data_to_csv'):
        df = transform_data(raw_data, filename)
        assert len(df) == unique_length, "Duplicates should be removed"


def test_transform_data_convert_dates(raw_data):
    filename = 'test_file.csv'
    with patch('builtins.open', new_callable=mock_open), \
            patch('src.data_flow.main.load_data_to_csv'):
        df = transform_data(raw_data, filename)
        assert df['date'].dtype == '<M8[ns]', "'date' column should be datetime"


def test_transform_data_output_consistency(raw_data):
    filename = 'test_file.csv'
    with patch('builtins.open', new_callable=mock_open), \
            patch('src.data_flow.main.load_data_to_csv'):
        df = transform_data(raw_data, filename)
        assert df.iloc[0]['id'] == 1
        assert df.iloc[0]['name'] == 'Track1'
        assert df.iloc[0]['artist'] == 'Artist1'

def test_extract_items_success():
    with patch('requests.get') as mock_get:
        mock_get.return_value = Mock()
        mock_get.return_value.status_code = 200
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: mock_extract_page1),
            Mock(status_code=200, json=lambda: mock_extract_page2)
        ]

        items = extract_items('test')

        expected_items = [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            {"id": 3, "name": "item3"},
            {"id": 4, "name": "item4"}
        ]

        assert items == expected_items
        assert mock_get.call_count == 2

def test_extract_items_http_error():
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 404
        items = extract_items('test')
        assert items == []
        mock_get.assert_called_once()

def test_transform_data():
    transformed_df = transform_data(mock_transform, 'test.csv')
    assert not transformed_df.empty
    assert transformed_df.iloc[0]['user_id'] == 77206
    assert transformed_df.iloc[0]['items'] == (76995, 79174, 70046, 244, 12480)
    assert pd.to_datetime(transformed_df.iloc[0]['created_at']) == pd.to_datetime("2023-06-04T06:10:28")
    assert pd.to_datetime(transformed_df.iloc[0]['updated_at']) == pd.to_datetime("2024-05-15T07:04:43")

def test_load_data_create_file():
    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["Track1", "Track2"]
    })
    folder = "test_folder"
    file_name = "test_file.csv"
    filepath = os.path.join(folder, file_name)

    with patch("os.makedirs") as mock_makedirs, \
            patch("pandas.DataFrame.to_csv") as mock_to_csv, \
            patch("pandas.read_csv", side_effect=FileNotFoundError) as mock_read_csv:
        load_data_to_csv(df, folder, file_name)

        mock_makedirs.assert_called_once_with(folder, exist_ok=True)
        mock_to_csv.assert_called_once_with(filepath, index=False)

        assert mock_read_csv.call_count == 1

def test_load_data_existing_file():
    existing_df = pd.DataFrame({
        "id": [1, 2],
        "name": ["Track1", "Track2"]
    })

    new_df = pd.DataFrame({
        "id": [2, 3],
        "name": ["Track2", "Track3"]
    })

    combined_df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Track1", "Track2", "Track3"]
    }).reset_index(drop=True)

    folder = "test_folder"
    file_name = "test_file.csv"
    filepath = os.path.join(folder, file_name)

    with patch("os.makedirs") as mock_makedirs, \
            patch("pandas.read_csv", return_value=existing_df) as mock_read_csv, \
            patch("pandas.DataFrame.to_csv") as mock_to_csv:
        load_data_to_csv(new_df, folder, file_name)

        mock_makedirs.assert_called_once_with(folder, exist_ok=True)
        mock_read_csv.assert_called_once_with(filepath)

        final_df = pd.concat([existing_df, new_df]).drop_duplicates().reset_index(drop=True)
        assert final_df.equals(combined_df), "Appended DataFrame should not have duplicates"

        mock_to_csv.assert_called_once_with(filepath, index=False)