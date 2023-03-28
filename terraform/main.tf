terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = file("../credentials/zoomcamp-374100-a34bc7914122.json")

  project = var.project_id
  region  = var.region
  zone    = "southamerica-east1-a"
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "project_agro"
  location                    = var.region

  labels = {
    env = "default"
  }
}

resource "google_storage_bucket" "data_lake" {
  name          = "data_lake_${var.project_id}"
  location      = var.region

}
