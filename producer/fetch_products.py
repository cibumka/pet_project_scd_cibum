"""Raw-layer producer: pulls products from the Fake Store API and lands
them as Parquet in the MinIO "raw" bucket, partitioned by ingestion date.
"""
import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests

FAKESTORE_PRODUCTS_URL = "https://fakestoreapi.com/products"


def fetch_products() -> list[dict]:
    resp = requests.get(FAKESTORE_PRODUCTS_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )


def upload_products_parquet(products: list[dict], ds: str, bucket: str = "raw") -> str:
    df = pd.json_normalize(products)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)

    key = f"products/dt={ds}/products.parquet"
    _s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return f"s3://{bucket}/{key}"


def fetch_and_upload(ds: str) -> str:
    products = fetch_products()
    uri = upload_products_parquet(products, ds)
    print(f"Wrote {len(products)} products to {uri}")
    return uri


if __name__ == "__main__":
    fetch_and_upload(ds=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
