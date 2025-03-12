import boto3 
import argparse
import yaml

def parser_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", 
                            required=True, 
                            default='*',
                            help="Path to the YAML config file")
    parser.add_argument("--endpoint", 
                        required=True, 
                        default='*',
                        help="Endpoint where the bucket will be created")
    parser.add_argument("--bucket",   
                    required=True,  
                    default='*',    
                    help="Bucket destination")   



    return parser.parse_args()


if __name__ == '__main__':
    parser = parser_arguments()
    path = './output/'

    # Initing boto3 client with info in ~/.aws/*
    session = boto3.Session(profile_name=parser.profile)
    client = session.client('s3', endpoint_url=parser.endpoint)

    try:
        objs = client.list_objects_v2(Bucket=parser.bucket)
        downloaded_objs = [o['Key'] for o in objs['Contents']]
        parquet_list = [o for o in objs if '.parquet' in o]
        
        for obj in parquet_list:
            print(f"Downloading obj: {obj}")
            response = client.download_file(Bucket = parser.bucket, Key=obj, Filename = path + obj)

    except Exception as e:
        raise e

            
            
