"""Module contents scripts for extract data from API."""
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile

import httpx
import pandas as pd


class Extract:
    def __init__(self):
        self._get_faostat_data()
        self._get_info_countries_data()

    def _get_faostat_data(self):
        """
        Extract infos about plantations from API
        """
        response = httpx.get(
            "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Data.zip"
        )
        zip_file = ZipFile(BytesIO(response.content))

        self.dataset_flags = pd.read_csv(
            zip_file.open("Production_Crops_Livestock_E_Flags.csv")
        )
        self.dataset_item_codes = pd.read_csv(
            zip_file.open("Production_Crops_Livestock_E_ItemCodes.csv"),
            encoding_errors="ignore",
        )
        self.dataset_area_codes = pd.read_csv(
            zip_file.open("Production_Crops_Livestock_E_AreaCodes.csv"),
            encoding_errors="ignore",
        )
        self.all_data_faostat = pd.read_csv(
            zip_file.open("Production_Crops_Livestock_E_All_Data.csv"),
            encoding_errors="ignore",
        )

    def _get_info_countries_data(self):
        """
        Extract infos about countries from API
        """
        response = httpx.get(
            "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Population1JanuaryBySingleAgeSex_Medium_1950-2021.zip"
        )
        zip_file = ZipFile(BytesIO(response.content))

        self.infos_countries = pd.read_csv(
            zip_file.open(
                "WPP2022_Population1JanuaryBySingleAgeSex_Medium_1950-2021.csv"
            )
        )

    def save_data_raw(self):
        """
        Save data raw in google storage bucket
        """
        now = datetime.now()
        date_time_str = now.strftime("%m-%d-%Y")

        dict_files_name = {
            "path_file_faostat": date_time_str + "/" + "faostat.parquet",
            "path_file_countries": date_time_str + "/" + "countries.parquet",
            "path_file_flags": date_time_str + "/" + "flags.parquet",
            "path_file_area_codes": date_time_str + "/" + "area_codes.parquet",
            "path_file_item_codes": date_time_str + "/" + "item_codes.parquet",
        }

        self.infos_countries["Notes"] = self.infos_countries["Notes"].astype("str")

        self.all_data_faostat.to_parquet(
            "gs://data_raw_area_project_agro/" + dict_files_name["path_file_faostat"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.infos_countries.to_parquet(
            "gs://data_raw_area_project_agro/" + dict_files_name["path_file_countries"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_flags.to_parquet(
            "gs://data_raw_area_project_agro/" + dict_files_name["path_file_flags"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_area_codes.to_parquet(
            "gs://data_raw_area_project_agro/"
            + dict_files_name["path_file_area_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_item_codes.to_parquet(
            "gs://data_raw_area_project_agro/"
            + dict_files_name["path_file_item_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )

        return dict_files_name
