import argparse
from google.cloud import dataproc_v1 as dataproc
from google.cloud.dataproc_v1 import JobControllerClient
from google.cloud.dataproc_v1.types import Job, PySparkJob
from google.cloud import storage
import creds
import google.api_core.exceptions
from google.protobuf import duration_pb2
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds.credential_path
import logging
logging.basicConfig(level=logging.DEBUG)

def create_cluster():
    # Create a client for the Data Proc API
    
    cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{creds.region}-dataproc.googleapis.com:443"})

    try:
        # Try to get the cluster
        print(creds.cluster_name)
        cluster_client.get_cluster(project_id=creds.project_id, region=creds.region, cluster_name=creds.cluster_name)
        # If the cluster exists, return its name

    except google.api_core.exceptions.NotFound:
        #cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
        print("cluster not found _ will create",creds.cluster_name)
        # Define the cluster configuration
        cluster = {
            "project_id": creds.project_id,
            "cluster_name": creds.cluster_name,  # Set a name for the cluster
            "config": {
                "gce_cluster_config": {
                    "zone_uri": creds.zone  # Set the zone where the cluster will be created
                },
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n1-standard-2"  
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-2"  
                },
                "secondary_worker_config": {
                    "num_instances": 0,  # Set the number of secondary worker instances to 0
                    "machine_type_uri": "n1-standard-2"  
                },
                "lifecycle_config": {
                    "auto_delete_ttl": duration_pb2.Duration(seconds=int(creds.auto_delete_ttl)*60)
                } if creds.auto_delete_ttl is not None else {}
            }
        }

        # Create the cluster
        operation = cluster_client.create_cluster(project_id=creds.project_id, region=creds.region, cluster=cluster)
        result = operation.result()
        print(result.cluster_name)
        #return result.cluster_name
    except Exception as error:
        print(f"ERROR *** Exception: {str(error)}")
        
def upload_input_file(input_file_path, bucket_name):
    # Create a client for Cloud Storage
    
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Upload the input file to the bucket
    blob = bucket.blob(input_file_path)  # Set the desired name for the input file in the bucket
    blob.upload_from_filename(input_file_path)

    #return 

def delete_cluster(cluster_name):
    # Create a client
    print("deleting now ")
    client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{creds.region}-dataproc.googleapis.com:443"})

    # Initialize request argument(s)
    request = dataproc.DeleteClusterRequest(
        project_id=creds.project_id,
        region=creds.region,
        cluster_name=creds.cluster_name,
    )
    # Make the request
    response = client.delete_cluster(request=request)

    # Handle the response
    print(response)


def main():
    parser = argparse.ArgumentParser(description="Submit a PySpark job to Google Cloud Dataproc.")
    parser.add_argument("-i","--input_file", help="Path to the input file to be processed.")
    parser.add_argument("-o","--output_directory", help="Path to the output directory.")
    parser.add_argument("-b","--bucket_name", help="bucket name to be used")
    parser.add_argument("-c","--cluster_name", help="cluster_name to be used/created")
    parser.add_argument("-d","--delete", help="y for delete after use, n for keep cluster alive")
    parser.add_argument("-ttl","--auto_delete_ttl", help="  ")
    args = parser.parse_args()
    if args.output_directory:
        creds.output_file=args.output_directory
    if args.bucket_name:
        creds.bucket_name=args.bucket_name
    if args.cluster_name:
        creds.cluster_name=args.cluster_name  
    if args.auto_delete_ttl:
        creds.auto_delete_ttl=args.auto_delete_ttl
    # Upload the input file to the bucket
    upload_input_file(args.input_file, creds.bucket_name)
    print("uploaded input file")
   
            
    # Create a client for the Data Proc API
    job_client = dataproc.JobControllerClient(client_options={"api_endpoint": f"{creds.region}-dataproc.googleapis.com:443"})

    # Auto-create the cluster
    create_cluster()
    #rint("created cluster:",cluster_name)

    # Define the PySpark job
    job = Job(
        placement=dataproc.JobPlacement(cluster_name=creds.cluster_name),
        pyspark_job=PySparkJob(
            main_python_file_uri=creds.main_python_file,
            args=["gs://{}/{}".format(creds.bucket_name,args.input_file), creds.output_file.format("output")]
        ),
    )

    # Submit the job to the cluster
    operation = job_client.submit_job_as_operation(request={"project_id": creds.project_id, "region": creds.region, "job": job})
    result = operation.result()
    print("job submitted - Job ID:", result.reference.job_id)
    if args.delete == "y":    
        delete_cluster(creds.cluster_name)
    else:
        print ("cluster not deleted") 
    
if __name__ == "__main__":
    main()
