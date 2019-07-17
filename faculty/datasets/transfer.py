import requests

from faculty.clients.object import CloudStorageProvider, CompletedUploadPart


MEGABYTE = 1024 * 1024


def download(object_client, project_id, datasets_path, local_path):
    """Download a file from the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to upload to in the object store
    local_path : str
        The local path of the object to upload
    """

    url = object_client.presign_download(project_id, datasets_path)

    with requests.get(url, stream=True) as response:
        with open(local_path, "wb") as fp:
            for chunk in response.iter_content():
                if chunk:  # Filter out keep-alive chunks
                    fp.write(chunk)


def upload(object_client, project_id, datasets_path, local_path):
    """Upload a file to the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to upload to in the object store
    local_path : str
        The local path of the object to upload
    """

    presign_response = object_client.presign_upload(project_id, datasets_path)

    if presign_response.provider == CloudStorageProvider.S3:
        _s3_upload(
            object_client,
            project_id,
            datasets_path,
            local_path,
            presign_response.upload_id,
        )
    elif presign_response.provider == CloudStorageProvider.GCS:
        _gcs_upload(presign_response.url, local_path)
    else:
        raise ValueError(
            "Unsupported cloud storage provider: {}".format(
                presign_response.provider
            )
        )


def _file_chunk_iterator(local_path, chunk_size=5 * MEGABYTE):
    with open(local_path, "rb") as fp:
        chunk = fp.read(chunk_size)
        while chunk:
            yield chunk
            chunk = fp.read(chunk_size)


def _s3_upload(
    object_client, project_id, datasets_path, local_path, upload_id
):

    completed_parts = []

    for i, chunk in enumerate(_file_chunk_iterator(local_path)):

        part_number = i + 1

        chunk_url = object_client.presign_upload_part(
            project_id, datasets_path, upload_id, part_number
        )

        upload_response = requests.put(chunk_url, data=chunk)

        completed_parts.append(
            CompletedUploadPart(
                part_number=part_number, etag=upload_response.headers["ETag"]
            )
        )

    object_client.complete_multipart_upload(
        project_id, datasets_path, upload_id, completed_parts
    )


def _gcs_upload(upload_url, local_path):
    raise NotImplementedError("GCS upload is not implemented")
