"""Module contents scripts for land data in Bigquery."""
import pandas as pd
from google.oauth2.service_account import Credentials


class Landing:
    """
    Stage land data into DW
    """

    def __init__(self, dict_files_name):
        self.dict_files_name = dict_files_name
        self._load_dataset()

    def _load_dataset(self):
        """
        Load dataset in memory from google storage bucket
        """

        self.all_data_faostat = pd.read_parquet(
            "gs://data_lake_zoomcamp-374100/data_staging/"
            + self.dict_files_name["path_file_faostat"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        ).astype(
            {
                "id": "str",
                "code_country": "int64",
                "year": "int64",
                "unit": "str",
                "value": "float64",
                "flag": "str",
                "description_flag": "str",
                "code_item": "int64",
                "name_element": "str",
            }
        )

        self.infos_countries = pd.read_parquet(
            "gs://data_lake_zoomcamp-374100/data_staging/"
            + self.dict_files_name["path_file_countries"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        ).astype(
            {
                "id": "str",
                "id_country": "int64",
                "year": "int64",
                "value": "float64",
                "footnotes": "str",
            }
        )

        self.dataset_area_codes = pd.read_parquet(
            "gs://data_lake_zoomcamp-374100/data_staging/"
            + self.dict_files_name["path_file_area_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        ).astype({"id": "int64", "name": "str"})

        self.dataset_item_codes = pd.read_parquet(
            "gs://data_lake_zoomcamp-374100/data_staging/"
            + self.dict_files_name["path_file_item_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        ).astype({"id": "int64", "name": "str"})

    def insert_in_bq(self):
        """
        Insert data in DW
        """
        credentials = Credentials.from_service_account_file(
            "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
        )

        self.all_data_faostat.to_gbq(
            destination_table="project_agro.Elements",
            project_id="zoomcamp-374100",
            if_exists="replace",
            credentials=credentials,
        )

        self.infos_countries.to_gbq(
            destination_table="project_agro.Population",
            project_id="zoomcamp-374100",
            if_exists="replace",
            credentials=credentials,
        )

        self.dataset_area_codes.to_gbq(
            destination_table="project_agro.Countries",
            project_id="zoomcamp-374100",
            if_exists="replace",
            credentials=credentials,
        )

        self.dataset_item_codes.to_gbq(
            destination_table="project_agro.Item",
            project_id="zoomcamp-374100",
            if_exists="replace",
            credentials=credentials,
        )
