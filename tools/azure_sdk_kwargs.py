"""
Generate keyword argument docstring for pass on parameters to azure sdk
"""

import azure.storage.blob
import azure.storage.blob.aio
import azure.identity.aio
import io

import docstring_parser


if __name__ == "__main__":

    # List of all invocations from spec.py of azure.storage.blob methods. Explicitly provided arguments to be excluded
    # as second item in tuple.

    methods = [
        (azure.identity.aio.DefaultAzureCredential, []),
        (azure.storage.blob.aio.BlobServiceClient, ["account_url"]),
        (
            azure.storage.blob.ContainerClient.get_container_properties,
            [],
        ),  # via container exists
        (
            azure.storage.blob.BlobServiceClient.list_containers,
            ["include_metadata"],
        ),  # via ls
        (
            azure.storage.blob.ContainerClient.walk_blobs,
            ["include", "name_starts_with"],
        ),  # via ls
        (
            azure.storage.blob.ContainerClient.list_blobs,
            ["include", "name_starts_with", "results_per_page"],
        ),  # via ls
        (azure.storage.blob.BlobServiceClient.create_container, ["name"]),  # via mkdir
        (azure.storage.blob.ContainerClient.delete_blob, ["blob"]),  # via rm
        (
            azure.storage.blob.BlobServiceClient.delete_container,
            ["container"],
        ),  # via rmdir
        (
            azure.storage.blob.BlobClient.get_blob_properties,
            ["version_id"],
        ),  # via isfile
        (azure.storage.blob.BlobClient.exists, ["version_id"]),
        (
            azure.storage.blob.BlobClient.download_blob,
            ["offset", "length", "version_id"],
        ),
        (
            azure.storage.blob.BlobClient.upload_blob,
            ["data", "overwrite", "metadata", "raw_response_hook"],
        ),
        (azure.storage.blob.BlobClient.start_copy_from_url, ["source_url"]),
    ]

    method_params = list(
        map(lambda _: (*_, docstring_parser.parse(_[0].__doc__).params), methods)
    )

    # Check correctly listed explicitly provided arguments
    # Manually check that missing arguments are due to incorrect docstring or docstring parser.
    unkown_args = {}
    for _ in method_params:
        arg_names = [*map(lambda param: param.arg_name, _[-1])]
        for arg_name in _[1]:
            if arg_name not in arg_names:
                unkown_args[_[0]] = arg_name

    lookup = {}

    # parameters interfering with explicitly provided arguments
    excludes = ["version_id", "match_condition", "credential", "results_per_page"]

    basic = ["timeout", "tags"]

    order = {v: n for n, v in enumerate(basic)}

    for _ in method_params:
        for param in _[-1]:
            if param.arg_name not in _[1] and param.arg_name not in excludes:
                lookup.setdefault(
                    param.arg_name,
                    {"methods": [], "types": [], "defaults": [], "descriptions": []},
                )
                lookup[param.arg_name]["methods"].append(_[0].__qualname__)
                if param.type_name not in lookup[param.arg_name]["types"]:
                    lookup[param.arg_name]["types"].append(
                        (param.type_name or "Any").replace("~", "")
                    )
                if param.default not in lookup[param.arg_name]["defaults"]:
                    lookup[param.arg_name]["defaults"].append(param.default)
                description = param.description
                if description not in lookup[param.arg_name]["descriptions"]:
                    lookup[param.arg_name]["descriptions"].append(description)

    def sort_rule(kv):
        prefix1 = str(order.get(kv[0], 999999)).zfill(6)
        prefix2 = "10000".zfill(6)
        if "BlobServiceClient" in kv[1]["methods"]:
            prefix2 = "0".zfill(6)
        if "DefaultAzureCredential" in kv[1]["methods"]:
            prefix2 = "1".zfill(6)
        return f"{prefix1}-{prefix2}-{kv[0]}"

    indent = " " * 4

    data = {
        key: {
            "methods": "".join(
                [f"\n{2 * indent}- {v}" for v in sorted(value["methods"])]
            ),
            "type": value["types"][0],
            "description": value["descriptions"][0],
        }
        for key, value in sorted(lookup.items(), key=sort_rule)
    }

    def doc_reference(key):
        if key == "DefaultAzureCredential":
            return "https://learn.microsoft.com/en-us/python/api/azure-identity/?view=azure-python (version 1.12.0)"
        return (
            "Description from https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob"
            "?view=azure-python (version 12.13.1) "
        )

    docstring = io.StringIO()
    for key, value in data.items():
        docstring.write(f"{indent}{key}")
        if value["type"] != "Any":
            docstring.write(f": {value['type']}")
        docstring.write("\n")
        if key not in basic:
            docstring.write(2 * indent + "Advanced settings parameter.\n")
        docstring.write(
            2 * indent + f"Directly passed to azure-sdk calls: {value['methods']}.\n"
        )
        docstring.write(2 * indent + f"{doc_reference(key)}:\n")
        description = value["description"].replace("\n", f"\n{2 * indent}")
        docstring.write(f"{2 * indent}{description}" + "\n")

    print("Use this in to filter azure_sdk_kwargs in AzureBlobFileSystem\n")

    print(excludes)

    print("\nAppend this to AzureBlobFileSystem docstring\n")

    print(docstring.getvalue())
