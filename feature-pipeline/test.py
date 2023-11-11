
import datetime
from feature_pipeline.etl.extract import from_file

API_URL = "https://drive.google.com/uc?export=download&id=1y48YeDymLurOTUO-GeFOUXVNc9MCApG5"


test_df, test_metadata = from_file(
    export_end_reference_datetime = datetime.datetime(2022, 11, 6, 22, 0, 0)
)

print(test_df.head())
print(test_metadata)