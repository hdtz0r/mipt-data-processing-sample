from logging import Logger
from os import cpu_count
from concurrent.futures import Future, ProcessPoolExecutor, Executor, ThreadPoolExecutor, wait
from typing import List
from config.configuration import Configuration
from datastore.datastore import Datastore
from datastore.factory import DatastoreFactory
from logging_utils import WithLogger, time_and_log
from models.generic_model import GenericModel
from providers.data_provider import DataProvider
from providers.datasource import Datasource
from reflection.dymanic_import_utils import import_class
from transformers.generic_transformer import GenericTransformer


class DataProcessor(metaclass=WithLogger):

    _task_name: str = None
    _datastore_factory: DatastoreFactory = None
    _data_provider: DataProvider = None
    _data_executor: Executor = None
    _transformer_configuration: Configuration = None
    _max_processes: int = 1
    _max_io_workers: int = 1

    def __init__(self, name: str, data_provider: DataProvider, datastore_factory: Datastore, configuration: Configuration) -> None:
        self._task_name = name
        self._data_provider = data_provider
        self._datastore_factory = datastore_factory
        self._max_processes = configuration.property(
            "max-processes", cpu_count())
        self._max_io_workers = configuration.property(
            "max-io-workers", round(cpu_count() / 2))
        self._data_executor = ProcessPoolExecutor(
            max_workers=self._max_processes+1)
        self._transformer_configuration = Configuration(
            configuration.property("transformer", {}))

    @time_and_log
    def process(self):
        self.info(f"Initialize datasource for ELT process {self}")

        datastore = self._datastore_factory.create()
        datastore.initialize()
        datastore.setup()
        datastore.close()

        with self._data_provider:
            self.info(
                f"Start processing total {self._data_provider.total()} datasources")
            total_slices = self._max_processes
            batch_size = round(self._data_provider.total() / total_slices)
            batch_size = 1 if batch_size <= 1 else batch_size
            batches: List[List[Datasource]] = []

            for _ in range(total_slices):
                batches.append([])

            batches.append([])

            current_batch = 0
            for datasource in self._data_provider.datasources():
                batches[current_batch].append(datasource)

                if len(batches[current_batch]) == batch_size:
                    current_batch = current_batch + 1

            transformer = self._create_transformer()

            for batch in batches:
                if batch:
                    self._data_executor.submit(
                        DataProcessor._process_datasource_internal, self.getLogger(), self._max_io_workers, self._datastore_factory, batch, transformer)

            self._data_executor.shutdown()
        self.info(f"ETL process {self} is completed")

    def _create_transformer(self) -> GenericTransformer:
        transformer = None
        if self._transformer_configuration:
            transformer_class_name = self._transformer_configuration.property(
                "transformer", "GenericTransformer")
            transformer_class = import_class(
                "transformers", transformer_class_name)
            transformer = transformer_class(Configuration(
                self._transformer_configuration.property("parameters", {})))
        else:
            transformer_class = import_class(
                "transformers",  "GenericTransformer")
            transformer = transformer_class(Configuration({}))
        return transformer

    @staticmethod
    def _process_datasource_internal(logger: Logger, max_workers: int, datastore_factory: DatastoreFactory, datasources: List[Datasource], transformer: GenericTransformer):
        try:
            datastore = datastore_factory.create()
            io_datasource_executor = ThreadPoolExecutor(
                max_workers=max_workers)

            current_datasource_slice = datasources
            while current_datasource_slice:
                futures: List[Future[List[GenericModel]]] = []
                for i in range(max_workers):
                    if i < len(current_datasource_slice):
                        futures.append(io_datasource_executor.submit(
                            DataProcessor._process_datasource, logger, current_datasource_slice[i], datastore, transformer))
                    else:
                        break

                wait(futures)

                if max_workers > len(current_datasource_slice):
                    break
                else:
                    current_datasource_slice = current_datasource_slice[max_workers:]
            datastore.close()
        except Exception as ex:
            logger.warn(f"Could not create datastore cause {ex}")

    @staticmethod
    def _process_datasource(logger: Logger, datasource: Datasource, datastore: Datastore, transformer: GenericTransformer) -> List[GenericModel]:
        try:
            datasource.load()
            for slice in datasource.slices():
                for data in slice:
                    try:
                        model = transformer.transform(data)
                        if model.validate():
                            if model.filter():
                                model.save(datastore)
                            else:
                                logger.debug("Data record was filtered")
                        else:
                            logger.debug("Data model is invalid")
                    except Exception as ex:
                        logger.error(
                            f"Could not transform data model {data}", ex)

            datasource.release()
        except Exception as ex:
            logger.warn(f"Could not load datasource {datasource} cause {ex}")

    def __str__(self) -> str:
        return f"{self._task_name.upper()}-ETL-PROCESS"
