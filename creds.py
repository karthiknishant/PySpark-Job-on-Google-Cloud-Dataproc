# Set your Google Cloud project ID and region where the cluster is located
project_id = "Your Project ID"
region = "Your region"
zone="https://www.googleapis.com/compute/v1/projects/{}/zones/Your Zone".format(project_id)
credential_path="Your keyfile path"
# Set the cluster name and the bucket where the input/output data is stored
cluster_name = "cluster name you want"
bucket_name = "bucket name you want to use"

# Set the PySpark job details
main_python_file = "gs://{}/path to pyspark job code".format(bucket_name)
input_file = "gs://{}/path to input file".format(bucket_name)
output_file = "gs://{}/output directory you want".format(bucket_name)

auto_delete_ttl=None
