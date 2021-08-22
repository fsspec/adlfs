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


def get_max_concurrency():
    """
    Attempt to determine the number of logical cores
    available to the process and return
    """
    import os
    try:
        num = len(os.sched_getaffinity(0))
        num = num * 2 # Let's assume two threads per core
    except AttributeError:
        # This returns the # of logical processors 
        # (or threads) available in the machine
        # but not necessarily the # available to the process
        num = os.cpu_count()  # Here we will assume the process has a quarter of the threads
        num = num // 4
    except:  # noqa: E722
        num = 1
    num = int(num)
    return num 