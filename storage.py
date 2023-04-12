# Imports necessary to read files installed on azure.
from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.filedatalake import FileSystemClient

# Allows pyarrow to treat Azure like a pyarrow Filesystem.
# See https://arrow.apache.org/docs/python/filesystems.html

# import pyarrow.fs
# from pyarrow import Table
# import pyarrow.parquet as pq
# import pyarrowfs_adlgen2

def get_azure_file(storage_account: str, storage_container: str, client_id: str, file_path):
    storage_account_name = storage_account
    mi_credential = ManagedIdentityCredential(
      client_id=client_id
    )
    credential = ChainedTokenCredential(mi_credential)
    destination = storage_container
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=destination)
    dscs_file = file_system_client.get_file_client(file_path)
    file_system_client.create_directory(directory="myoutput")
    new_file = file_system_client.get_file_client("myoutput/" + file_path)
    new_file.upload_data(data=dscs_file.download_file().readall(),overwrite=True)
    #return dscs_file.download_file().readall()

import sys
print(sys.argv[4])
get_azure_file(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])