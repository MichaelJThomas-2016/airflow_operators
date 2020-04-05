import io
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from googleapiclient.discovery import build
from oauth2client import client
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import re
import logging


class DFAtoBigQueryOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 service_account,
                 email,
                 profile_id,
                 report_id,
                 destination_dataset_table,
                 project_id,
                 table_schema=None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.service_account = service_account
        self.profile_id = profile_id
        self.email = email
        self.report_id = report_id,
        self.destination_dataset_table = destination_dataset_table,
        self.project_id = project_id
        self.table_schema = table_schema
        self.api = "dfareporting"
        self.api_version = "v3.3"
        self.api_scopes = ["https://www.googleapis.com/auth/dfareporting",
                           "https://www.googleapis.com/auth/dfatrafficking"]
        self.retry_interval = 30
        self.report_split = "Report Fields"
        self.chunk_size = 32 * 1024 * 1024
        self.max_retry_elapsed_time = 60 * 10
        self.service = self.build_service()

    @staticmethod
    def column_clean(df):
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns=lambda x: re.sub('[^a-zA-Z0-9]', '_', x), inplace=True)
        df.rename(columns=lambda x: re.sub('__*', '_', x), inplace=True)
        df.columns = [x[1:] if x.startswith('_') else x for x in df.columns]
        df.columns = [x[:-1] if x.endswith('_') else x for x in df.columns]
        df.columns = [x.lower() for x in df.columns]
        return df

    def sleep_next_interval(self, sleep):
        return sleep + self.retry_interval

    def build_service(self):
        creds = self.service_account
        creds = creds.with_scopes(self.api_scopes)
        service = build(self.api, self.api_version, credentials=creds)
        return service

    def execute(self, context):
        try:
            report_file = self.service().reports().run(profileId=self.profile_id,
                                                       reportId=self.report_id).execute()

            sleep = 0
            start_time = time.time()
            fh = io.BytesIO()
            while True:
                report_file = self.service().files().get(reportId=self.report_id,
                                                         profileId=self.profile_id)

                status = report_file.get('status')
                file_id = report_file.get('id')

                if status == "REPORT_AVAILABLE":
                    request = self.service.files().get_media(reportId=self.report_id, fileId=file_id)
                    downloader = MediaIoBaseDownload(fh, request, self.chunk_size)

                    download_finished = False
                    while download_finished is False:
                        _, download_finished = downloader.next_chunk()

                    file_value = fh.getvalue()
                    file_str = file_value.decode('utf-8')
                    *_, data = file_str.split(self.report_split)
                    df = pd.read_csv(io.StringIO(data))
                    df = self.column_clean(df)
                    if self.table_schema:
                        df.to_gbq(destination_dataset_table=self.destination_dataset_table,
                                  project_id=self.project_id,
                                  table_schema=self.table_schema)
                        return
                    df.to_gbq(destination_dataset_table=self.destination_dataset_table,
                              project_id=self.project_id)
                    return
                elif status != "PROCESSING":
                    logging.error("Report {} has failed".format(self.report_id))
                    return

                elif time.time() - start_time > self.max_retry_elapsed_time:
                    logging.error("Report {} has timed out".format(self.report_id))

                sleep = self.sleep_next_interval(sleep)
                logging.info("File still downloading. Sleeping for {}".format(sleep))
        except client.AccessTokenRefreshError:
            logging.error('The credentials have been revoked or expired, please re-run'
                          'to re-authorize')
