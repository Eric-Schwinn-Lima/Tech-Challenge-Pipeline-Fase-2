"""Purge all versions and delete markers from the project's versioned S3 bucket.

Use only for the bucket name returned by this project's Terraform output. This is destructive.
"""
from __future__ import annotations
import argparse
import boto3

parser = argparse.ArgumentParser(description="Apaga objetos, versões e delete markers de um bucket S3 versionado.")
parser.add_argument("--bucket", required=True)
parser.add_argument("--region", default="sa-east-1")
parser.add_argument("--confirm", action="store_true", help="Confirma a exclusão irreversível.")
args = parser.parse_args()
if not args.confirm:
    raise SystemExit("Operação destrutiva. Execute novamente com --confirm depois de conferir o bucket.")
s3 = boto3.client("s3", region_name=args.region)
paginator = s3.get_paginator("list_object_versions")
count = 0
for page in paginator.paginate(Bucket=args.bucket):
    objects = []
    for item in page.get("Versions", []):
        objects.append({"Key": item["Key"], "VersionId": item["VersionId"]})
    for item in page.get("DeleteMarkers", []):
        objects.append({"Key": item["Key"], "VersionId": item["VersionId"]})
    for start in range(0, len(objects), 1000):
        batch = objects[start:start + 1000]
        if batch:
            s3.delete_objects(Bucket=args.bucket, Delete={"Objects": batch, "Quiet": True})
            count += len(batch)
print(f"Excluídas {count} versões/delete markers de s3://{args.bucket}")
