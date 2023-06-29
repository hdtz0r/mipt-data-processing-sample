from typing import List
from config.provider import Configuration, configuration
from data_processor import DataProcessor
from datastore.factory import DatastoreFactory
from logging_utils import WithLogger
from providers.data_provider import DataProvider
from reflection.dymanic_import_utils import import_class


class Application(metaclass=WithLogger):

    _configuration: Configuration = None
    _etl_processes: List[DataProcessor] = []

    def __init__(self, configuration: Configuration):
        self._configuration = configuration
        for name, process_configuration in configuration.each("processes"):
            if process_configuration.property("disabled"):
                continue

            datasource_configuration = Configuration(
                process_configuration.property("datasource"))
            datastore_configuration = Configuration(
                process_configuration.property("datastore"))

            if not datastore_configuration or not datasource_configuration:
                self.warn(
                    f"The {name}'s process configuration is mess. U should configure both datastore and datasource")
            else:
                try:
                    data_provider = self._create_data_provider(datasource_configuration)
                    connection_url = datastore_configuration.property("connection-url")
                    driver = datastore_configuration.property("driver")
                    parameters = Configuration(datastore_configuration.property("parameters", {}))
                    datastore_factory = DatastoreFactory(name, driver, connection_url, parameters)
                    self._etl_processes.append(DataProcessor(name, data_provider, datastore_factory, Configuration(process_configuration.property("processor", {}))))
                except Exception as ex:
                    self.error(f"Could not initialize {name} etl process", ex)

    def _create_data_provider(self, configuration: Configuration) -> DataProvider:
        provider_class_name = configuration.property("provider", "FileDataProvider")
        provider_class = import_class("providers", provider_class_name)
        return provider_class(configuration)

    def run(self):
        self.info(
            f"Processing total {len(self._etl_processes)} ETL processes...")
        for processor in self._etl_processes:
            try:
                processor.process()
            except Exception as ex:
                self.error(f"Could not perfome ETL process {processor}", ex)
        self.info("All ETL processes are completed")


if __name__ == "__main__":
    application: Application = Application(configuration)
    application.run()
