import re

from fsspec.asyn import maybe_sync


async def filter_blobs(blobs, target_path):
    """
    Filters out blobs that do not come from target_path

    Parameters
    ----------
    blobs:  A list of candidate blobs to be returned from Azure

    target_path: Actual prefix of the blob folder
    """
    finalblobs = [
        b for b in blobs if re.search(r"\b" + target_path + r"(?=/)" + r"\b", b["name"])
    ]
    return finalblobs

async def get_blob_metadata(container_client, path):
    async with container_client.get_blob_client(path) as bc:
        properties = await bc.get_blob_properties()
        if 'metadata' in properties.keys():
            metadata = properties['metadata']
        else:
            metadata = None
    return metadata
