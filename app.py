from google.cloud import bigquery
from google.cloud import bigquery_storage
import streamlit as st
import pydata_google_auth
from google.oauth2 import service_account

scopes = ['https://www.googleapis.com/auth/bigquery']


with st.sidebar.expander('Operations'):
    operations = ['Select', 'Authenticate', 'Use service account']
    operation = st.sidebar.selectbox('Choose your operation', operations)

if operation == 'Select':
    st.error('Choose your operation to proceed')
    st.stop()
if operation == 'Authenticate':
    credentials = pydata_google_auth.get_user_credentials(
        scopes,
        # client_id=client_id,
        # client_secret=client_secret,
        use_local_webserver=True,
    )

    project_id = "moz-fx-data-bq-regrets-report"
    bq_client = bigquery.Client(project=project_id, credentials=credentials)
    bq_storage_client = bigquery_storage.BigQueryReadClient(
        credentials=credentials)
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

    report_list = bq_client.query(report_query).result(
    ).to_dataframe(bqstorage_client=bq_storage_client)
    st.dataframe(report_list.head())
if operation == 'Use service account':
    credentials = service_account.Credentials.from_service_account_info(
        dict(**st.secrets.bq_service_account), scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "regrets-reporter-dev"
    bq_client = bigquery.Client(project=project_id, credentials=credentials)
    #bq_storage_client = bigquery_storage.BigQueryReadClient(
    #    credentials=credentials)
    report_query = f'''
    SELECT * FROM `regrets-reporter-dev.ra_can_read.pair_test_real`

    '''
    report_list = bq_client.query(report_query).result(
    ).to_dataframe()
    st.dataframe(report_list.head())