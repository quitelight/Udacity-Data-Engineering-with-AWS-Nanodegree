# Import the boto3 library to interface with AWS Services programmatically
import boto3
import os
from pathlib import Path
import configparser
import pandas as pd
from IPython.display import display
import time

# Ensure before running, that the AWS secret and access key has been generated
# Instructions:
# 1.    Create a new IAM user in your AWS account
# 2.    Give it AdministratorAccess, From Attach existing policies directly Tab
# 3.    Take note of the access key and secret
# 4.    Edit the file dwh.cfg in the same folder as this notebook and fill
#       Ensure the following is in the *.dwh configuration file
#       [AWS]
#       KEY= YOUR_AWS_KEY
#       SECRET= YOUR_AWS_SECRET
# 5.    Run this script to create the Redshift cluster

def main():
    # Load parameters from dwh.cfg file
    path = Path(__file__)
    ROOT_DIR = path.parent.absolute()
    config_path = os.path.join(ROOT_DIR, "dwh.cfg")
    config = configparser.ConfigParser()
    config.read(config_path)

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION = config.get('DEFAULT', 'region')
    DWH_CLUSTER_TYPE = config.get("CLUSTER", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("CLUSTER", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("CLUSTER", "DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("CLUSTER", "dbname")
    DWH_DB_USER = config.get("CLUSTER", "user")
    DWH_DB_PASSWORD = config.get("CLUSTER", "password")
    DWH_PORT = config.get("CLUSTER", "port")
    DWH_IAM_ROLE_NAME = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")


    # Get and print the IAM role ARN
    roleArn = "arn:aws:iam::225552118750:role/dwhRole"
    print("roleArn is {} \n".format(roleArn))


    # Setup Redshift client
    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    # Create Redshift cluster
    try:
        print("Beginning creating Redshift cluster. Please wait.")
        response = redshift.create_cluster(
            # add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # add parameter for role (to allow s3 access)
            IamRoles=[roleArn]
        )

    except Exception as e:
        print(e)

    return redshift, DWH_CLUSTER_IDENTIFIER


# Show cluster details once available
def checkRedshiftCluster(redshift, identifier):
    redshift, DWH_CLUSTER_IDENTIFIER = redshift, identifier
    cluster_status = "Not available"
    increment = 0
    print("Cluster Status: {}".format(str(cluster_status).title()))
    while cluster_status != "available":
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

        pd.set_option('display.max_colwidth', None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                      "NumberOfNodes", 'VpcId']
        x = [(k, v) for k, v in myClusterProps.items() if k in keysToShow]
        cluster_status = x[2][1]
        if cluster_status == "creating" and increment == 1:
            continue
        elif cluster_status == "creating" and increment == 0:
            print("Cluster Status: {}".format(str(cluster_status).title()))
            increment += 1
        else:
            print("Cluster Status: {}\n".format(str(cluster_status).title()))
            print("See cluster details below:")
            # Convert the dataframe as string and display in CLI
            # The display() function makes it possible to display a pandas dataframe in
            # the CLI
            display(pd.DataFrame(data=x, columns=["Key", "Value"]))
            # Display ARN and cluster endpoint
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
            print("Cluster endpoint is :: ", DWH_ENDPOINT)
            print("Cluster role ARN :: ", DWH_ROLE_ARN)
            break


if __name__ == '__main__':
    start_time = time.monotonic()
    redshift, identifier = main()
    checkRedshiftCluster(redshift, identifier)
    end_time = time.monotonic()
    print(f"\nRedshift cluster has been created in {round((end_time - start_time), 2)} seconds.")