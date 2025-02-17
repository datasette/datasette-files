#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3",
# ]
# ///
import sys
import argparse
import boto3
import botocore


def create_bucket(bucket_name, region):
    s3_client = boto3.client("s3", region_name=region)
    try:
        if region == "us-east-1":
            # In us-east-1 no LocationConstraint is needed.
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"Bucket '{bucket_name}' created successfully in region '{region}'.")
    except botocore.exceptions.ClientError as error:
        print(f"Error creating bucket: {error}")
        sys.exit(1)


def configure_cors(bucket_name, region):
    s3_client = boto3.client("s3", region_name=region)
    # This CORS configuration allows all origins to perform GET, PUT, POST, DELETE, and HEAD requests.
    cors_configuration = {
        "CORSRules": [
            {
                "AllowedHeaders": ["*"],
                "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
                "AllowedOrigins": ["*"],
                "MaxAgeSeconds": 3000,
            }
        ]
    }
    try:
        s3_client.put_bucket_cors(
            Bucket=bucket_name, CORSConfiguration=cors_configuration
        )
        print("CORS configuration applied successfully.")
    except botocore.exceptions.ClientError as error:
        print(f"Error setting CORS configuration: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Create an S3 bucket with CORS configuration for presigned uploads."
    )
    parser.add_argument("bucket", help="Name of the S3 bucket to create.")
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region to create the bucket in (default: us-east-1).",
    )
    args = parser.parse_args()

    create_bucket(args.bucket, args.region)
    configure_cors(args.bucket, args.region)


if __name__ == "__main__":
    main()
