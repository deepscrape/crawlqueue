from datetime import datetime
import os
from typing import Any, Dict
import aioboto3
import zstandard as zstd
import pydantic


class TigrisBucketResult(pydantic.BaseModel):
    key_name: str
    file_name: str
    file_size: float = pydantic.Field(
        ge=0
    )  # ensures file size is non-negative in kilobytes
    file_compressed_size: float = pydantic.Field(ge=0)
    created_at: datetime = pydantic.Field(default_factory=datetime.now)

    def to_str(self) -> str:
        """Return a formatted string representation"""
        return (
            f"File: {self.file_name}\n"
            f"Key: {self.key_name}\n"
            f"Size: {self.file_size} KB\n"
            f"Created: {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def to_json(self) -> str:
        """Return JSON string"""
        return self.model_dump_json()

    def to_dict(self) -> Dict[str, Any]:
        """Return dictionary representation"""
        return self.model_dump()


# from botocore.exceptions import DataNotFoundError, UnknownServiceError

# Tigris Buckets S3-compatible configuration
TIGRIS_BUCKET_NAME = "deepcrawl4.bucket.a"
TIGRIS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
TIGRIS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TIGRIS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL_S3")  # Change if needed


# Initialize S3 client for Tigris
session = aioboto3.Session()


async def folder_exists(folder_name: str) -> bool:
    """Check if a folder exists in Tigris Buckets."""
    try:
        async with session.client(
            "s3",
            endpoint_url=TIGRIS_ENDPOINT_URL,
            aws_access_key_id=TIGRIS_ACCESS_KEY,
            aws_secret_access_key=TIGRIS_SECRET_KEY,
        ) as svc:
            # List buckets
            response = await svc.list_buckets()
            for bucket in response["Buckets"]:
                print(f'  {bucket["Name"]}')

            # List objects
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME, Prefix=f"{folder_name}/", MaxKeys=1
            )
            for obj in response["Contents"]:
                print(f'  {obj["Key"]}')

        return "Contents" in response  # Returns True if folder has at least one file
    except Exception as e:
        print(f"Error checking folder existence: {e}")
        return False


async def upload_markdown(
    markdown_str: str, folder_name: str, file_name: str
) -> TigrisBucketResult | None:
    """Uploads a Markdown string directly to Tigris Buckets if it's >= 100 KB."""

    file_size_kb = len(markdown_str.encode("utf-8")) / 1024  # Convert bytes to KB

    # if file_size_kb < 100:
    #     print(
    #         f"⚠️ Skipping upload: {file_name} is only {file_size_kb:.2f} KB (less than 100 KB)"
    #     )
    # return  # Skip upload if file size is less than 100 KB

    key_name = f"{folder_name}/{file_name}"  # E.g., "markdown-files/report.md"

    try:
        async with session.client(
            "s3",
            endpoint_url=TIGRIS_ENDPOINT_URL,
            aws_access_key_id=TIGRIS_ACCESS_KEY,
            aws_secret_access_key=TIGRIS_SECRET_KEY,
        ) as svc:
            # Zstd (Balanced for Speed & Compression)
            # Pros: Faster and better compression than Gzip.
            # Cons: Less widely supported than Gzip.
            compressor = zstd.ZstdCompressor()
            compressed_data = compressor.compress(markdown_str.encode("utf-8"))
            file_compressed_size = len(compressed_data) / 1024  # Convert bytes to KB
            print(f"Compressed {file_name} to {file_compressed_size:.2f} KB")

            await svc.put_object(
                Bucket=TIGRIS_BUCKET_NAME,
                Key=key_name,
                Body=compressed_data,
                ContentType="text/markdown",
            )
            print(
                f"✅ Uploaded {file_name} to {TIGRIS_BUCKET_NAME}/{key_name} ({file_size_kb:.2f} KB)"
            )
            return TigrisBucketResult(
                key_name=key_name,
                file_name=file_name,
                file_size=file_size_kb,
                file_compressed_size=file_compressed_size,
            )
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return None
