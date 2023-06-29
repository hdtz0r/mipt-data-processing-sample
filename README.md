# Overview

The result data set contains 20917 entries

|ogrn|inn|kpp|fullname|okvd_code|
|---|---|---|---|---|
|1031100403828|1101036090|110101001|ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "МЕТОДА-РОСТ"|61.10.4|
|1212200013618|2234016095|223401001|ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "АЛТНЭТ"|61.10|
|1202500027927|2508139077|250801001|ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "СИТРОНИКС"|61.10|
|1137847500009|7810405255|781001001|ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ДЕЛЬТА"|61.10|
|1062310039000|2310119546|231001001|ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "НЬЮЛАЙН"|61.10.1|

External data links

1. [okved](https://ofdata.ru/open-data/download/okved_2.json.zip)
2. [egrul](https://ofdata.ru/open-data/download/egrul.json.zip)

# Basic requirements

Atleast Python 3.11.x

Requirements pyyaml & orjson

```bash
pip install -r requiremenets.txt
```

# Configuration

The default one is generated automaticaly on startup if it does not exists

<details>

<summary>Default configuration</summary>

```yaml
log:
  file-name: sample-application.log
  file-path: ./
  level: 20 # log level
  max-part-size-in-bytes: 819200 # the size of log file
  max-size-in-bytes: 8388608 # the total size of logs
processes: # describes a set of ETL process
  hw1: # The name of etl process and its definition
    disabled: false # A flag that indicates whether this process is ignored
    datasource:
      data-path: ./data/hw1/ # A folder that contais an external data that is stored as a bunch of json files or zip files
      provider: FileDataProvider # Concrete implementation that serves data from file system
    datastore:
      connection-url: hw1.db # The format depends on driver implementation
      driver: SqlLiteDatastore # Concrete datastore implementation
      parameters: # Driver options
        batch:
          max-size: 5000
        connection: {} # The specific driver connection options
        init-storage-sql: "\n                                CREATE TABLE IF NOT EXISTS\
          \ hw1 ( \n                                    code TEXT PRIMARY KEY, \n\
          \                                    parent_code TEXT, \n              \
          \                      section TEXT, \n                                \
          \    name TEXT, \n                                    comment TEXT);\n \
          \                               DELETE FROM hw1;\n                     \
          \       " # The init datastore sql script
        max-pool-size: 1 # The connection pool size. For sqllite driver there is no parallel access
        # NOTE do not rise this value for sqllite dirver in order to avoid perfomance loss caused by join calls
    processor:
      max-processes: 1 # The total process to use. Default fallbaks to cpu_count
      max-io-workers: 1 # The total thread to perfome read io
      # NOTE each worker perfome datasource load into process memory so choose this value wisely. Default fallbaks to cpu_count / 2
      transformer:
        parameters:
          model: EconomicActivityKindEntry # The model implementation to use 
        transformer: GenericTransformer # The strategy implementation to convert raw data into desired model
  hw2: # The name of etl process and its definition
    disabled: false # A flag that indicates whether this process is ignored
    datasource:
      data-path: ./data/hw2/ # A folder that contais an external data that is stored as a bunch of json files or zip files
    datastore:
      connection-url: hw1.db # The format depends on driver implementation
      driver: SqlLiteDatastore # Concrete datastore implementation
      parameters: # Driver options
        batch:
          max-size: 5000
        connection: {} # The specific driver connection options
        init-storage-sql: "\n                                CREATE TABLE IF NOT EXISTS\
          \ telecom_companies ( \n                                    ogrn TEXT PRIMARY\
          \ KEY, \n                                    inn TEXT, \n              \
          \                      kpp TEXT, \n                                    fullname\
          \ TEXT, \n                                    okvd_code TEXT);\n       \
          \                         DELETE FROM telecom_companies;\n             \
          \               " # The init datastore sql script
        max-pool-size: 1 # The connection pool size. For sqllite driver there is no parallel access
        # NOTE do not rise this value for sqllite dirver in order to avoid perfomance loss caused by join calls
    processor:
      max-processes: 12 # The total process to use. Default fallbaks to cpu_count
      max-io-workers: 4 # The total thread to perfome read io
      # NOTE each worker perfome datasource load into process memory so choose this value wisely. Default fallbaks to cpu_count / 2
      # NOTE For this sample datasource an app consumes 2.2GB memory in total
      transformer:
        parameters:
          model: TelecomCompany # The model implementation to use 
        transformer: GenericTransformer # The strategy implementation to convert raw data into desired model
```

</details>

# Launching

activate venv and type 

```bash
cd ./src & python app.py
```

Test system overview

| Caption | DeviceID | MaxClockSpeed | Name | NumberOfCores | Status |
| --- | --- | --- | --- | --- | --- |
| Intel64 Family 6 Model 165 Stepping 5 | CPU0 | 2904 | Intel(R) Core(TM) i5-10400F CPU @ 2.90GHz | 6 | OK |

<details>

<summary>Below is the normal log output</summary>

```log
2023-06-26 20:24:59,981: [INFO] [MainProcess] [MainThread] [Application.class]  -       Processing total 2 ETL processes...
2023-06-26 20:24:59,981: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       Initialize datasource for ELT process HW1-ETL-PROCESS
2023-06-26 20:24:59,991: [INFO] [MainProcess] [MainThread] [DatastoreConnectionPool.class]      -       Initializing datastore hw1 using pool size 1 
2023-06-26 20:24:59,994: [INFO] [MainProcess] [MainThread] [SqlLiteDatastore.class]     -       Datastore hw1.db is ready
2023-06-26 20:25:01,125: [INFO] [MainProcess] [MainThread] [DatastoreConnectionPool.class]      -       Shutdown pool for datastore hw1
2023-06-26 20:25:01,127: [INFO] [MainProcess] [MainThread] [DatastoreConnectionPool.class]      -       Datastores hw1 pool is terminated
2023-06-26 20:25:01,130: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       Start processing total 1 datasources     
2023-06-26 20:25:02,741: [INFO] [SpawnProcess-1] [MainThread] [DatastoreConnectionPool.class]   -       Shutdown pool for datastore hw1
2023-06-26 20:25:02,742: [INFO] [SpawnProcess-1] [MainThread] [DatastoreConnectionPool.class]   -       Datastores hw1 pool is terminated
2023-06-26 20:25:02,758: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       ETL process HW1-ETL-PROCESS is completed
2023-06-26 20:25:02,759: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       Method invokation DataProcessor->process tooks 2.7779922485351562 seconds
2023-06-26 20:25:02,760: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       Initialize datasource for ELT process HW2-ETL-PROCESS
2023-06-26 20:25:02,761: [INFO] [MainProcess] [MainThread] [DatastoreConnectionPool.class]      -       Initializing datastore hw2 using pool size 1
2023-06-26 20:25:02,763: [INFO] [MainProcess] [MainThread] [SqlLiteDatastore.class]     -       Datastore C:/Users/Swedenteen/hw1.db is ready
2023-06-26 20:25:02,770: [INFO] [MainProcess] [MainThread] [DatastoreConnectionPool.class]      -       Shutdown pool for datastore hw2  
2023-06-26 20:25:02,770: [INFO] [MainProcess] [MainThread] [DatastoreConnectionPool.class]      -       Datastores hw2 pool is terminated
2023-06-26 20:25:02,931: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       Start processing total 11457 datasources
2023-06-26 20:30:58,276: [INFO] [SpawnProcess-7] [MainThread] [DatastoreConnectionPool.class]   -       Shutdown pool for datastore hw2
2023-06-26 20:30:58,280: [INFO] [SpawnProcess-7] [MainThread] [DatastoreConnectionPool.class]   -       Datastores hw2 pool is terminated
...
2023-06-26 20:31:04,839: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       ETL process HW2-ETL-PROCESS is completed
2023-06-26 20:31:04,840: [INFO] [MainProcess] [MainThread] [DataProcessor.class]        -       Method invokation DataProcessor->process tooks 362.07990741729736 seconds
2023-06-26 20:31:04,841: [INFO] [MainProcess] [MainThread] [Application.class]  -       All ETL processes are completed
```

</details>

# Extending

## Datasource

To implement a new data store firstly define a new class that is derived from DatastoreConnection and override appropriate methods

```python

class MysqlConnection(DatastoreConnection):

    def open(self):
        pass

    def bulk_insert(self, query: BulkInsert):
        pass

    def execute_script(self, sql: str):
        pass

    def execute(self, sql_query: str, **kwargs: any) -> any:
        pass

    def test(self) -> bool:
        return False

    def close(self):
        pass

```

Then define a data store class itself that is derived from Datastore and override appropriate methods 

Dont forget to override _set_connection_pool_factory method that specifies a connection impl to use

```python
class MysqlDatastore(Datastore):

    def _set_connection_pool_factory(self) -> Type[DatastoreConnection]:
        return MysqlConnection

    def bulk_insert(self, sql_query: str, **kwargs: any):
        pass

    def insert(self, sql_query: str, **kwargs: any) -> Future[int]:
        return self._accuire_and_perfome(lambda conn: SqlLiteConnection._do_execute(sql_query, kwargs, conn))

```

Then use it in urs configuration

```yaml
 datastore:
    connection-url:
    driver: MysqlDatastore
    parameters:
```

## Data providers

To implement a new data provider firstly define a new class that is derived from DataProvider and override appropriate methods

```python
class PostgresDataProvider(DataProvider):

    # TODO register datasource
    def initialize(self):
        self.add_datasource(...)
   
```

Impl appropriate content loader, a class that is derived from ContentLoader

```python
class MyCustomPostgresDataProvider(ContentLoader):

    # TODO fetch real data
    def load(self) -> List[Dict[str, any]]:
        pass
   
```

Call 

```python
self.add_datasource(Datasource("person_view", MyCustomPostgresDataProvider(...)))
```

to register an external data source that is lazly loaded by design

Then use it in urs configuration

```yaml
datasource:
  provider: PostgresDataProvider
```

## Models

To implement a new model define new class that is derived from GenercModel and override appropriate methods

```python
class CustomModel(DataContainer):

    def save(self, storage: Datastore):
        pass

    def validate(self) -> bool:
        return False
    
    def filter(self) -> bool:
        return True

```

Then use it in urs configuration

```yaml
processor:
  transformer:
    parameters:
      model: CustomModel
```