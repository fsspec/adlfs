import re


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
