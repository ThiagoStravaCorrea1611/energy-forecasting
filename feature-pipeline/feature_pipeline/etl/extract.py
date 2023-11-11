# Libraries
import datetime
from json import JSONDecodeError
from pathlib import Path
from pandas.errors import EmptyDataError
from typing import Any, Dict, Tuple, Optional
import pandas as pd
import requests
from yarl import URL

# Parameters
API_URL = "https://drive.google.com/uc?export=download&id=1y48YeDymLurOTUO-GeFOUXVNc9MCApG5"

from feature_pipeline import utils, settings

logger = utils.get_logger(__name__)

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

