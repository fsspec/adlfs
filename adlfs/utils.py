async def filter_blobs(blobs, target_path, delimiter="/"):
    """
    Filters out blobs that do not come from target_path

    Parameters
    ----------
    blobs:  A list of candidate blobs to be returned from Azure

    target_path: Actual prefix of the blob folder

    delimiter: str
            Delimiter used to separate containers and files
    """
    # remove delimiter and spaces, then add delimiter at the end
    target_path = target_path.strip(" " + delimiter) + delimiter
    finalblobs = [
        b for b in blobs if b["name"].strip(" " + delimiter).startswith(target_path)
    ]
    return finalblobs


async def get_blob_metadata(container_client, path):
    async with container_client.get_blob_client(path) as bc:
        properties = await bc.get_blob_properties()
        if "metadata" in properties.keys():
            metadata = properties["metadata"]
        else:
            metadata = None
    return metadata


async def close_service_client(fs):
    """
    Implements asynchronous closure of service client for
    AzureBlobFile objects
    """
    await fs.service_client.close()


async def close_container_client(file_obj):
    """
    Implements asynchronous closure of container client for
    AzureBlobFile objects
    """
    await file_obj.container_client.close()
