import datetime
import sys
import time

from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_container_sas,
    generate_blob_sas
)
from azure.mgmt.batch import (
    BatchManagementClient 
)
from azure.batch import (
    BatchServiceClient
)
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as taskbatch
import azure.mgmt.batch.models as batchmodels
from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential

import config

def create_pool(batch_service_client: BatchManagementClient, pool_id: str, requirements: list):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print(f'Creating pool [{pool_id}]...')

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/    
    pool=batchmodels.Pool(
        identity= batchmodels.BatchPoolIdentity(
            type="UserAssigned",
            user_assigned_identities={
                "/subscriptions/32eb88b4-4029-4094-85e3-ec8b7ce1fc00/resourcegroups/batch-demo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/batch-storage" : batchmodels.UserAssignedIdentities()
            }
        ),
        display_name=config.POOL_ID,
        vm_size=config.POOL_VM_SIZE,        
        deployment_configuration=batchmodels.DeploymentConfiguration(
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="microsoft-dsvm",
                    offer="dsvm-win-2019",
                    sku="winserver-2019",
                    version="latest" 
                ),
                node_agent_sku_id="batch.node.windows amd64"
            ) 
        ),
        scale_settings=batchmodels.ScaleSettings(
            fixed_scale=batchmodels.FixedScaleSettings(
                target_dedicated_nodes=1,
                target_low_priority_nodes=0 
            )
        ),
        task_slots_per_node=1,
        start_task=batchmodels.StartTask(command_line="powershell .\setup.ps1",resource_files=requirements),
    )
    '''
    id=pool_id,
        start_task=batchmodels.StartTask(command_line="powershell .\setup.ps1",resource_files=requirements),
        identity=batchmodels.BatchPoolIdentity(
            type="UserAssigned",
            user_assigned_identities={
                "/subscriptions/32eb88b4-4029-4094-85e3-ec8b7ce1fc00/resourcegroups/batch-demo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/batch-storage" : batchmodels.UserAssignedIdentities()
            }
        ),
        task_slots_per_node=2,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="microsoft-dsvm",
                offer="dsvm-win-2019",
                sku="winserver-2019",
                version="latest" 
            ),
            node_agent_sku_id="batch.node.windows amd64"),
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT 

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        start_task=batchmodels.StartTask(command_line="powershell .\setup.ps1",resource_files=requirements),
        task_slots_per_node=2,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="microsoft-dsvm",
                offer="dsvm-win-2019",
                sku="winserver-2019",
                version="latest" 
            ),
            node_agent_sku_id="batch.node.windows amd64"),
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT 
    )
    '''
    batch_service_client.pool.create(resource_group_name="batch-demo",account_name=config.BATCH_ACCOUNT_NAME,pool_name=config.POOL_ID,parameters=pool)

def create_job(batch_service_client: BatchManagementClient, job_id: str, pool_id: str):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print(f'Creating job [{job_id}]...')

    job = taskbatch.JobAddParameter(
        id=job_id,
        pool_info=taskbatch.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)

def add_tasks(batch_service_client: BatchServiceClient, job_id: str,fileName: str, storage_account: str, storage_container: str, client_id: str):
    command=f"python -c \"import os;import subprocess;os.chdir(os.environ.get('AZ_BATCH_NODE_SHARED_DIR') + '\\Azure-Batch-Python');subprocess.call('git pull');os.system('python storage.py {storage_account} {storage_container} {client_id} {fileName}')\""
    batch_service_client.task.add(job_id, taskbatch.TaskAddParameter(command_line=command,id=fileName.split(".")[0]))

def wait_for_tasks_to_complete(batch_service_client: BatchServiceClient, job_id: str,
                               timeout: datetime.timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :param job_id: The id of the job whose tasks should be to monitored.
    :param timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != taskbatch.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True

        time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))

#credentials = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
serviceCred = SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
        config.BATCH_ACCOUNT_KEY)

batch_service = BatchServiceClient(
    serviceCred,
    batch_url=config.BATCH_ACCOUNT_URL
)

create_job(batch_service,sys.argv[1],config.POOL_ID)

#create_pool(batch_client, config.POOL_ID,resourceFiles)
for x in sys.argv[2:]:
  add_tasks(batch_service,sys.argv[1],x,config.STORAGE_ACCOUNT,config.STORAGE_CONTAINER,config.CLIENT_ID)

wait_for_tasks_to_complete(batch_service,sys.argv[1],datetime.timedelta(minutes=30))