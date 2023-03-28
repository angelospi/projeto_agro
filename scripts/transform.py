from datetime import datetime

import pandas as pd


class Transform:
    def __init__(self, dict_files_name):
        self.infos_countries = None
        self.dataframe_years_flag = None
        self.all_data_faostat = None

        self.dict_files_name = dict_files_name
        self._load_dataset()

    def _load_dataset(self):
        """
        Load dataset in memory from google storage bucket
        """
        self.all_data_faostat = pd.read_parquet(
            "gs://data_raw_area_project_agro/"
            + self.dict_files_name["path_file_faostat"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.infos_countries = pd.read_parquet(
            "gs://data_raw_area_project_agro/"
            + self.dict_files_name["path_file_countries"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_flags = pd.read_parquet(
            "gs://data_raw_area_project_agro/"
            + self.dict_files_name["path_file_flags"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_area_codes = pd.read_parquet(
            "gs://data_raw_area_project_agro/"
            + self.dict_files_name["path_file_area_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_item_codes = pd.read_parquet(
            "gs://data_raw_area_project_agro/"
            + self.dict_files_name["path_file_item_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )

    def separate_data_faostat(self):
        """
        Separate dataset faostat for organize columns to line
        """
        list_years_flag = []
        for ano in range(1961, 2022):
            list_years_flag.append("Y" + str(ano) + "F")

        columns_years_flag = [
            *list_years_flag,
            *["Area Code (M49)", "Item Code", "Element Code"],
        ]
        self.dataframe_years_flag = self.all_data_faostat[columns_years_flag]

        self.all_data_faostat = self.all_data_faostat.drop(columns=list_years_flag)

    def transform_columns(self):
        """
        Transform columns for patterns
        """
        self.all_data_faostat = self.all_data_faostat.melt(
            id_vars=[
                "Area Code",
                "Area Code (M49)",
                "Area",
                "Item Code",
                "Item Code (CPC)",
                "Item",
                "Element Code",
                "Element",
                "Unit",
            ],
            var_name="Year",
            value_name="Value",
        )

        self.all_data_faostat["Area Code (M49)"] = self.all_data_faostat[
            "Area Code (M49)"
        ].apply(lambda value: value.replace("'", ""))
        self.all_data_faostat["Item Code (CPC)"] = self.all_data_faostat[
            "Item Code (CPC)"
        ].apply(lambda value: value.replace("'", ""))
        self.all_data_faostat["Year"] = self.all_data_faostat["Year"].apply(
            lambda value: value.replace("Y", "")
        )

        dict_types = {
            "Area Code": "int64",
            "Area Code (M49)": "int64",
            "Area": "str",
            "Item Code": "int64",
            "Item Code (CPC)": "str",
            "Item": "str",
            "Element Code": "int64",
            "Element": "str",
            "Unit": "str",
            "Year": "str",
            "Value": "str",
        }

        self.all_data_faostat.astype(dict_types)

        self.dataset_area_codes["M49 Code"] = self.dataset_area_codes["M49 Code"].apply(
            lambda value: value.replace("'", "")
        )

        self.dataframe_years_flag["Area Code (M49)"] = self.dataframe_years_flag[
            "Area Code (M49)"
        ].apply(lambda value: value.replace("'", ""))
        self.dataframe_years_flag["id"] = (
            self.dataframe_years_flag["Area Code (M49)"].astype("str")
            + "_"
            + self.dataframe_years_flag["Item Code"].astype("str")
            + "_"
            + self.dataframe_years_flag["Element Code"].astype("str")
        )
        self.dataframe_years_flag = self.dataframe_years_flag.melt(
            id_vars=["Area Code (M49)", "Item Code", "Element Code", "id"],
            var_name="Year",
            value_name="Flag",
        )

        self.dataframe_years_flag["Year"] = self.dataframe_years_flag["Year"].apply(
            lambda value: value.replace("Y", "")
        )
        self.dataframe_years_flag["Year"] = self.dataframe_years_flag["Year"].apply(
            lambda value: value.replace("F", "")
        )
        self.dataframe_years_flag["id"] = (
            self.dataframe_years_flag["Year"] + "_" + self.dataframe_years_flag["id"]
        )

        self.infos_countries["id"] = (
            self.infos_countries["Time"].astype("str")
            + "_"
            + self.infos_countries["LocID"].astype("str")
        )

    def clean_dataset(self):
        """
        Clean missing data and select data
        """
        self.all_data_faostat = self.all_data_faostat.drop(
            self.all_data_faostat[
                self.all_data_faostat.Element == "Producing Animals/Slaughtered"
            ].index
        )
        self.all_data_faostat = self.all_data_faostat.drop(
            self.all_data_faostat[self.all_data_faostat.Element == "Laying"].index
        )
        self.all_data_faostat = self.all_data_faostat.drop(
            self.all_data_faostat[self.all_data_faostat.Element == "Milk Animals"].index
        )
        self.all_data_faostat = self.all_data_faostat.drop(
            self.all_data_faostat[self.all_data_faostat.Element == "Prod Popultn"].index
        )

        self.all_data_faostat["id"] = (
            self.all_data_faostat["Year"]
            + "_"
            + self.all_data_faostat["Area Code (M49)"].astype("str")
            + "_"
            + self.all_data_faostat["Item Code"].astype("str")
            + "_"
            + self.all_data_faostat["Element Code"].astype("str")
        )

        self.all_data_faostat.dropna(subset=["Value"], inplace=True)

        self.dataframe_years_flag.drop(
            columns=["Area Code (M49)", "Item Code", "Element Code", "Year"],
            inplace=True,
        )

        self.infos_countries = self.infos_countries[
            self.infos_countries["LocTypeID"] == 4
        ]

    def merge_dataframes(self):
        """
        Merge dataframes
        """
        self.all_data_faostat = pd.merge(
            self.all_data_faostat, self.dataframe_years_flag, how="left", on="id"
        )
        self.all_data_faostat = pd.merge(
            self.all_data_faostat, self.dataset_flags, how="left", on="Flag"
        )

    def send_data_to_staging(self):
        """
        Send data with selected features to google storage bucket
        """
        self.all_data_faostat = self.all_data_faostat[
            [
                "id",
                "Year",
                "Unit",
                "Value",
                "Flag",
                "Description",
                "Item Code",
                "Area Code (M49)",
                "Element",
            ]
        ]
        self.infos_countries = self.infos_countries[
            [
                "id",
                "Notes",
                "LocID",
                "LocTypeID",
                "LocTypeName",
                "Location",
                "Time",
                "PopTotal",
            ]
        ]
        self.dataset_area_codes = self.dataset_area_codes[["M49 Code", "Area"]]
        self.dataset_item_codes = self.dataset_item_codes[["Item Code", "Item"]]

        now = datetime.now()
        date_time_str = now.strftime("%m-%d-%Y")

        dict_files_name = {
            "path_file_faostat": date_time_str + "/" + "faostat.parquet",
            "path_file_countries": date_time_str + "/" + "infos_countries.parquet",
            "path_file_area_codes": date_time_str + "/" + "area_codes.parquet",
            "path_file_item_codes": date_time_str + "/" + "item_codes.parquet",
        }

        self.all_data_faostat.to_parquet(
            "gs://data_staging_area_project_agro/"
            + dict_files_name["path_file_faostat"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.infos_countries.to_parquet(
            "gs://data_staging_area_project_agro/"
            + dict_files_name["path_file_countries"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_area_codes.to_parquet(
            "gs://data_staging_area_project_agro/"
            + dict_files_name["path_file_area_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
        self.dataset_item_codes.to_parquet(
            "gs://data_staging_area_project_agro/"
            + dict_files_name["path_file_item_codes"],
            storage_options={
                "token": "/opt/credentials/zoomcamp-374100-a34bc7914122.json"
            },
        )
