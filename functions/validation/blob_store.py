"""BlobStore (claims_lakehouse.validation.landing) on Azure Blob Storage.

Authenticates with the Function's managed identity (DefaultAzureCredential), so no keys.
"""

from __future__ import annotations

from pathlib import Path

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class AzureBlobStore:
    def __init__(self, account_url: str) -> None:
        self._service = BlobServiceClient(account_url, credential=DefaultAzureCredential())

    def download(self, container: str, path: str, target: Path, etag: str | None) -> bool:
        blob = self._service.get_blob_client(container, path)
        conditions = {}
        if etag:
            # Event Grid gives the ETag unquoted; HTTP If-Match wants it quoted.
            conditions = {
                "etag": f'"{etag.strip(chr(34))}"',
                "match_condition": MatchConditions.IfNotModified,
            }
        try:
            stream = blob.download_blob(**conditions)
            with target.open("wb") as f:
                stream.readinto(f)
        except (ResourceNotFoundError, ResourceModifiedError):
            return False
        return True

    def create(self, container: str, path: str, source: Path) -> bool:
        blob = self._service.get_blob_client(container, path)
        try:
            with source.open("rb") as f:
                blob.upload_blob(f, overwrite=False)
        except ResourceExistsError:
            return False
        return True

    def put(self, container: str, path: str, source: Path | bytes) -> None:
        blob = self._service.get_blob_client(container, path)
        if isinstance(source, bytes):
            blob.upload_blob(source, overwrite=True)
        else:
            with source.open("rb") as f:
                blob.upload_blob(f, overwrite=True)
