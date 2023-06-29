from config.configuration import Configuration
from datastore.operations.bulk_insert import BulkInsert
from logging_utils import WithLogger

class DatastoreConnection(metaclass=WithLogger):

    _connection_string = None
    _configuration: Configuration = None

    def __init__(self, connection_string: str, configuration: Configuration):
        self._connection_string = connection_string
        self._configuration = configuration

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
