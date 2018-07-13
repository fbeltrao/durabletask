 # Load testing Durable Task Framework backend extensions

The ConsoleTester application was designed to simulate a host running the durable task framework. There are two ways to initialize it:

#### Worker Mode
To start a host to execute pending orchestrations and tasks. In this mode the application runs forever or until &lt;ENTER&gt; is pressed
```console
dotnet ConsoleTester.dll worker {backendType} {workerId}
```

#### Start Orchestrations Mode
In this mode the host will start and create N orchestrations. The application runs until all N orchestrations are completed. There is a bug (yet to be solved) where the application never ends even though there are no more pending work to be done. To verify it look if both control and work item queues are empty.
```console
dotnet ConsoleTester.dll {backendType} {amountOfOrchestrationToExecute}
```


Possible backend types: 
- cosmosdb (default)
- sql 
- storage

# Testing

In order to properly load test the application follow this steps:
1. Ensure that your are using a release build
1. Set the application insights key in the App.Config file, otherwise no measurement will be stored (APPINSIGHTS_INSTRUMENTATIONKEY is the key)
1. Set the PartitionCount key in App.Config file to 16 if you want to see how well it scales with the maximum parallel hosts. Start the hosts by running the "start15{BackendType}.bat" file. This script will start hosts from the bin/release folder (you need to fix the hard-coded path). Once the CPU does down from starting all apps at once create the last host with the amount of work to be done (500 in most of my tests).

# Using CosmosDB backend

To use the CosmosDB backend you need to first create a CosmosDB account with a database called "durabletask". The collections will be created automatically by the host once started. If the collection already exists the host will not be dropped (useful if you wish to change the partition/RUs to test something in particular)

Set the following App.Config settings:

- **CosmosDBEndpoint**: https address of CosmosDB
- **CosmosDBAuthKey**: CosmosDB auth key
- **CosmosDBQueueUsePartition**: indicates if the **worker queue** should use a partition key
- **CosmosDBQueueUseOneCollectionPerQueueType**: indicates if worker and control queues should be different collection. Leave as "true"
- **CosmosDBLeaseManagementUsePartition**: indicates if the lease collection should be partitioned by task hub name. Leave as "true"

Run the a host once to create the required collection (some exceptions are not being threated yet). Ensure that the 5 collections have been created:

- queue-workitem
- queue-control
- unitTestLeastManagement
- loadTestinstance
- loadTesthistory

I know, the naming is great :).

# Using SQL Server backend

To use SQL Server backend you need to create a SQL Azure database, in premium tier (in order to use memory optimized tables). I suggest P2 to run this load test.
Unlike the CosmosDB backend the code does not create the SQL objects at start. Therefore you need to run the scripts manually. You can find them at src/DurableTask.CosmosDB/SQL. Run the following scripts:
- History.sql
- Instance.sql
- Lease.sql
- Queue_OnMemory.sql
- TaskHub.sql

Set the following App.Config settings:
- **SqlConnectionString**: Connection string to your Azure SQL database
- **SqlQueueUseMemoryOptimizedTable**: indicates if the memory optimized table implementation should be used. Leave as "true"

Known fact: the SQL implementation might not be prioritizing the queue items based on the time of queueing. There is not ```ORDER BY NextAvailableTime``` but I need to double check if that won't happen by default according to the index being used.

# Using Storage backend

To use Storage backend you need to create the Storage Account in Azure. V2 works fine. Queues, blobs and tables will be created at the start of the host execution.

Set the following App.Config settings:
- **StorageConnectionString**: Connection string to your Azure Storage account


# Running a load test step by step

To run a load test with 16 workers using CosmosDB as backend:

1. Use a VM in same region where CosmosDB resides to run the hosts
1. Set the App.Config CosmosDB properties
1. Set the partition count to 16
1. Build the ConsoleTester application in release
1. Run once to ensure all collections/stored procedures will be in place\
```dotnet ConsoleTester.dll cosmosdb worker worker01``` 
1. Start the **worker** hosts by running ./ConsoleTester/start15Cosmos.bat
1. Wait until the machine where the hosts are running CPU goes back to normal
1. Start the 16th host with that will also create the orchestrations\
```dotnet ConsoleTester.dll cosmosdb 500```

# Helper Application Insights queries

#### Comparing backend implementations that were executed in different timestamps
```
dependencies
| where (timestamp between(todatetime('2018-07-13T09:48:00.000') .. todatetime('2018-07-13T09:51:00.000'))) or (timestamp between(todatetime('2018-07-13T14:18:00.000') .. todatetime('2018-07-13T14:21:00.000')))
| where type != "SQL.Queue" and type != "CosmosDB.Queue" 
| project timestamp, type, duration, backendType=customDimensions.BackendType
| summarize avg(duration), min(duration), max(duration), count(1) by type, tostring(backendType)
| order by type, backendType
```


#### Getting RU/s from CosmosDB operations
```
dependencies
| where timestamp between(todatetime('2018-07-09T14:05:45.000') .. todatetime('2018-07-09T14:08:00.000')) and type=='CosmosDB.Queue' and success  == true 
| project timestamp, name, duration, backendType=customDimensions.BackendType, queueName=tostring(customDimensions.queueName), ru = todouble(replace("," , ".", tostring(customDimensions.RequestCharge)))
| summarize avg(duration), min(duration), max(duration), avg(ru), min(ru), max(ru), sum(ru), count(1) by name, queueName, tostring(backendType), bin(timestamp, 10sec)
| order by timestamp desc

```