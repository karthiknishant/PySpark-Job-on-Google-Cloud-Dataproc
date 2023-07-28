# PySpark Job on Google Cloud Dataproc

This project demonstrates how to submit a PySpark job to Google Cloud Dataproc using the Python client library. The script allows you to create a Dataproc cluster, upload input files to a Cloud Storage bucket, run a PySpark job on the cluster, and optionally delete the cluster after the job completion.

## Prerequisites

Before running the script, make sure you have the following:

1. A Google Cloud Platform (GCP) project with billing enabled.
2. Google Cloud SDK installed and configured with your GCP project.
3. Python 3 installed on your local machine.
4. Required Python packages installed. You can install them using the `requirements.txt` file:

`pip install -r requirements.txt`

## Setup
Set up your GCP credentials by following the instructions in the Google Cloud documentation.
Replace the placeholders in the creds.py file with your specific values:
-credential_path: Path to your GCP service account key file.

-project_id: Your GCP project ID.

-region: The region where you want to create the Dataproc cluster.

-zone: The zone where you want to create the Dataproc cluster.

-bucket_name: Name of the Cloud Storage bucket where the input file will be uploaded.

-cluster_name: Name of the Dataproc cluster to be created.

-main_python_file: Path to the main PySpark job file.

-output_file: Path to the output folder in the Cloud Storage bucket. This path should not already exist when you run the script.

-auto_delete_ttl: None if you dont want to exercise this option.

Usage
To submit a PySpark job to Dataproc, run the following command:

`python main.py -i input_file.csv -o output_directory -b your_bucket_name -c your_cluster_name -d y`

Replace the arguments with your specific values:


-i: Path to the input file to be processed.

-o: Path to the output directory.

-b: Name of your Cloud Storage bucket.

-c: Name of your Dataproc cluster.

-d: Use y to delete the cluster after the job, or n to keep the cluster alive.

-ttl :Time in minutes the cluster should be kept alive with no running job (minimum 10)

These arguments replace the ones in creds.py. None of them are mandatory.
