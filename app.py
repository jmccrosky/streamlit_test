from contextlib import contextmanager, redirect_stdout
from google.cloud import bigquery
from google.cloud import bigquery_storage
from io import StringIO
from time import sleep
import streamlit as st

@contextmanager
def st_capture(output_func):
    with StringIO() as stdout, redirect_stdout(stdout):
        old_write = stdout.write

        def new_write(string):
            ret = old_write(string)
            output_func(stdout.getvalue())
            return ret
        
        stdout.write = new_write
        yield

st.title("Streamlit test")
import pydata_google_auth
output = st.empty()
#with st_capture(output.code):
credentials = pydata_google_auth.get_user_credentials(
    ['https://www.googleapis.com/auth/bigquery'],
    use_local_webserver=True,
)
project_id = "moz-fx-data-bq-regrets-report"
bq_client = bigquery.Client(project=project_id, credentials=credentials)
bq_storage_client = bigquery_storage.BigQueryReadClient(credentials=credentials)
report_query = f'''
SELECT
  metrics.string.video_data_id AS video_id
FROM
  `moz-fx-data-shared-prod.regrets_reporter_ucs_stable.video_data_v1`
WHERE
  DATE(submission_timestamp) > "2021-12-2"
  AND MOD(FARM_FINGERPRINT(metrics.string.video_data_id), 1000) = 0
  
  GROUP BY metrics.string.video_data_id

'''


report_list = bq_client.query(report_query).result().to_dataframe(bqstorage_client=bq_storage_client)
st.dataframe(report_list.head())