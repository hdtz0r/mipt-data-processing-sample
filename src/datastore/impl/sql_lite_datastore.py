
from concurrent.futures import Future
from typing import Dict, Type
from config.configuration import Configuration
from datastore.connection.datastore_connection import DatastoreConnection
from datastore.connection.impl.sql_lite_connection import SqlLiteConnection
from datastore.datastore import Datastore
from datastore.operations.bulk_insert import BulkInsert

"""
    Represents an sql lite datastore
"""

class SqlLiteDatastore(Datastore):

    _insert_batch_size: int = None
    _bulk_insert: Dict[str, BulkInsert] = None

    def __init__(self, name: str, connection_string: str, configuration: Configuration):
        super().__init__(name, connection_string, configuration)
        self._insert_batch_size = configuration.property("batch.max-size", 5000)
        self._bulk_insert = {}

    def _set_connection_pool_factory(self) -> Type[DatastoreConnection]:
        return SqlLiteConnection

    def bulk_insert(self, sql_query: str, **kwargs: any):
        self._bulk_insert.setdefault(sql_query, BulkInsert(
            sql_query, self._insert_batch_size))

        bulk_insert = self._bulk_insert.get(sql_query)
        if bulk_insert.is_completed():
            del self._bulk_insert[sql_query]
            self._accuire_and_perfome(lambda conn: conn.bulk_insert(bulk_insert))
            self.bulk_insert(sql_query, **kwargs)
        else:
            bulk_insert.store(kwargs)

    def insert(self, sql_query: str, **kwargs: any) -> Future[int]:
        return self._accuire_and_perfome(lambda conn: SqlLiteConnection._do_execute(sql_query, kwargs, conn))

    @staticmethod
    def _do_execute(query: str, args: any, conn: DatastoreConnection):
        conn.execute(query, **args)

    def close(self):
        if self._bulk_insert:
            for _, v in self._bulk_insert.items():
                try:
                    self._accuire_and_perfome(lambda conn: conn.bulk_insert(v))
                except Exception as ex:
                    self.warn(f"Could not perfome batch insert cause {ex}")
        return super().close()
