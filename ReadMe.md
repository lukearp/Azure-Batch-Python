# What is in this repo?

Simple example of executing Python as Batch Tasks.  

# What is required? 

* Azure Storage Account with Hierarchical Namespaces (Data Lake) enabled.
* Storage container provisioned on the storage account with files in the root of the container.
* User Managed Identity with Blob Data Contributor permisions to the Data Lake.
* Azure Batch Pool created with the User Managed Identity and referencines setup.ps1. as a Start Task Example: powershell .\setup.ps1
  * Image used for Batch Pool is: Publisher = microsoft-dsvm, Offer = dsvm-win-2019, and Sku = 
winserver-2019.
* Create config.py with the following values in the directory of the Orchestration script

# Example config.py

```python
BATCH_ACCOUNT_NAME = ''  # Your batch account name
BATCH_ACCOUNT_KEY = ''  # Your batch account key
BATCH_ACCOUNT_URL = ''  # Your batch account URL

POOL_ID = ''  # Your Pool Name
JOB_ID = ''  # Job Name

CLIENT_ID = '' # User Managed Identity Client ID
STORAGE_ACCOUNT = '' # Name of storage account
STORAGE_CONTAINER = '' # Name of storage account container with files
```
# Example run of orchestrator.py

```python
pip install -r requirements.txt
python .\orchestrator.py jobname file.txt file2.txt file3.txt
```

This will create a job and 3 tasks associated.  The nodes will run storage.py and copy the files to an output folder on the same container.  In this example, file.txt, file2.txt, and file3.txt would need to be in the root of the storage container for the tasks to succeed.    