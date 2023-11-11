
import datetime
from feature_pipeline.etl.extract import _compute_extraction_window


test = _compute_extraction_window(export_end_reference_datetime = datetime.datetime(2022, 11, 11, 0, 0, 0),
                               days_delay =  15,
                               days_export =  6*30)

print(test)