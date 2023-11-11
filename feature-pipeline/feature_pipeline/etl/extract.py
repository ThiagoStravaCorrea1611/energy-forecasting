# Libraries
import datetime
from json import JSONDecodeError
from pathlib import Path
from pandas.errors import EmptyDataError
from typing import Any, Dict, Tuple, Optional
import pandas as pd
import requests
from yarl import URL

from feature_pipeline import utils, settings

# Parameters
API_URL = "https://drive.google.com/uc?export=download&id=1y48YeDymLurOTUO-GeFOUXVNc9MCApG5"

# Logging
logger = utils.get_logger(__name__)

def from_file(
    export_end_reference_datetime: Optional[datetime.datetime] = None,
    days_delay: int = 15,
    days_export: int = 30,
    url: str = API_URL,
    datetime_format: str = "%Y-%m-%d %H:%M",
    cache_dir: Optional[Path] = None,
) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    As the official API expired in July 2023, we will use a copy of the data to simulate the same behavior. 
    We made a copy of the data between '2020-06-30 22:00' and '2023-06-30 21:00'. Thus, there are 3 years of data to play with.
    
    Args:
        export_end_reference_datetime: The end reference datetime of the export window. If None, the current time is used.
            Because the data is always delayed with "days_delay" days, this date is used only as a reference point.
            The real extracted window will be computed as [export_end_reference_datetime - days_delay - days_export, export_end_reference_datetime - days_delay].
        days_delay: Data has a delay of N days. Thus, we have to shift our window with N days.
        days_export: The number of days to export.
        url: The URL of the API.
        datetime_format: The datetime format of the fields from the file.
        cache_dir: The directory where the downloaded data will be cached. By default it will be downloaded in the standard output directory.

    Returns:
          A tuple of a Pandas DataFrame containing the exported data and a dictionary of metadata.
    """

    export_start, export_end = _compute_extraction_window(export_end_reference_datetime=export_end_reference_datetime, days_delay=days_delay, days_export=days_export)
    records = _extract_records_from_file_url(url=url, export_start=export_start, export_end=export_end, datetime_format=datetime_format, cache_dir=cache_dir)
    
    metadata = {
        "days_delay": days_delay,
        "days_export": days_export,
        "url": url,
        "export_datetime_utc_start": export_start.strftime(datetime_format),
        "export_datetime_utc_end": export_end.strftime(datetime_format),
        "datetime_format": datetime_format,
        "num_unique_samples_per_time_series": len(records["HourUTC"].unique()),
    }

    return records, metadata

# Extract Functions
def _extract_records_from_file_url(url: str,
                                   export_start: datetime.datetime,
                                   export_end: datetime.datetime,
                                   datetime_format: str = "%Y-%m-%d %H:%M",
                                   cache_dir: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """Extract records from the file backup based on the given export window."""
    
    # In case no information is given it is assumed the data is in the default data folder
    if cache_dir is None:
        cache_dir = settings.OUTPUT_DIR / "data"
        cache_dir.mkdir(parents=True, exist_ok=True)
        
    file_path = cache_dir / "ConsumptionDE35Hour.csv"
    
    # Download the whole dataset file in case the file was not downloaded yet
    if not file_path.exists():
        logger.info(f"Downloading data from: {url}")

        try:
            response = requests.get(url)
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"Response status = {response.status_code}. Could not download the file due to: {e}"
            )

            return None
        
        if response.status_code != 200:
            raise ValueError(f"Response status = {response.status_code}. Could not download the file.")
        
        # Write the downloaded file to the expected folder
        with file_path.open("w") as f:
            f.write(response.text)

        logger.info(f"Successfully downloaded data to: {file_path}")
    else:
        logger.info(f"Data already downloaded at: {file_path}")
    
    # Load data as a pandas data frame
    try:
        data = pd.read_csv(file_path, delimiter=";")
    except EmptyDataError:
        file_path.unlink(missing_ok=True)
        
        raise ValueError(f"Downloaded file at {file_path} is empty. Could not load it into a DataFrame.")
    
    # Filter to return only the requested dates
    records = data[data["HourUTC"] >= export_start.strftime(datetime_format)][data["HourUTC"] < export_end.strftime(datetime_format)]

    return records


def _compute_extraction_window(export_end_reference_datetime: datetime.datetime,
                               days_delay: int,
                               days_export: int) -> Tuple[datetime.datetime, datetime.datetime]:
    """Compute the extraction window relative to 'export_end_reference_datetime' and take into
    consideration the maximum and minimum data points available in the dataset.
    export_end_reference_datetime: is the date we are interested in making the prediction for;
    days_delay: is the delay with which the data is available, hence the forecast horizon;
    days_export: the number of days we are interested to build our historic data set
    """
    # In case the export_end_reference_datetime is not available it is set to the last day available
    if export_end_reference_datetime is None:
        # As the dataset will expire in July 2023, we set the export end reference datetime to the last day of June 2023 + the delay.
        export_end_reference_datetime = datetime.datetime(
            2023, 6, 30, 21, 0, 0
        ) + datetime.timedelta(days=days_delay)
        
        export_end_reference_datetime = export_end_reference_datetime.replace(
            minute=0, second=0, microsecond=0
        )
    else:
        export_end_reference_datetime = export_end_reference_datetime.replace(
            minute=0, second=0, microsecond=0
        )
    
    # The last day available in the dataset
    expiring_dataset_datetime = datetime.datetime(2023, 6, 30, 21, 0, 0) + datetime.timedelta(
        days=days_delay
    )
    
    # Forcing the export_end_reference_datetime to be the last day available in the dataset in case it is bigger than it
    if export_end_reference_datetime > expiring_dataset_datetime:
        export_end_reference_datetime = expiring_dataset_datetime

        logger.warning(
            "We clapped 'export_end_reference_datetime' to 'datetime(2023, 6, 30) + datetime.timedelta(days=days_delay)' as \
        the dataset will not be updated starting from July 2023. The dataset will expire during 2023."
        )
        
    # The export ends days_delay before the reference date since this is the data available at the time of prediction
    export_end = export_end_reference_datetime - datetime.timedelta(days=days_delay)
    
    # The export begins days_delay+days_export before the reference date
    export_start = export_end_reference_datetime - datetime.timedelta(days=days_delay + days_export)
    
    # In case export_start is before the beginning of the dataset, the window is forced to match the first available
    min_export_start = datetime.datetime(2020, 6, 30, 22, 0, 0)
    if export_start < min_export_start:
        export_start = min_export_start
        export_end = export_start + datetime.timedelta(days=days_export)

        logger.warning(
            "We clapped 'export_start' to 'datetime(2020, 6, 30, 22, 0, 0)' and 'export_end' to \
        'export_start + datetime.timedelta(days=days_export)' as this is the first window available in the dataset."
        )
    
    return export_start, export_end


