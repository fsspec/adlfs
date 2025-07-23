HOST = "127.0.0.1:10000"
URL = f"http://{HOST}"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA
SAS_TOKEN = "not-a-real-sas-token"
DEFAULT_VERSION_ID = "1970-01-01T00:00:00.0000000Z"
LATEST_VERSION_ID = "2022-01-01T00:00:00.0000000Z"
