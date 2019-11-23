# Testing

adlfs uses the [Azurite][azurite] emulator for testing.
You need to start a docker container running Azurite with the following:

    docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 --debug /tmp/debug.log 

Alternatively, a `docker-compose.yml` file is provided in the root of the repo, which does the same thing.

    docker-compose up

[azurite]: https://github.com/Azure/Azurite