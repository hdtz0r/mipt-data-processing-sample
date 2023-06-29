from sqlite3 import Error, Connection, OperationalError, connect
from datastore.connection.datastore_connection import DatastoreConnection
from datastore.operations.bulk_insert import BulkInsert
from datastore.utils import retry

# TODO handle specific database errors


class SqlLiteConnection(DatastoreConnection):

    _connection: Connection = None

    def open(self):
        self.close()
        self._connection = connect(
            self._connection_string, check_same_thread=False)

    @retry(OperationalError)
    def bulk_insert(self, query: BulkInsert) -> int:
        if self._connection:
            try:
                cursor = self._connection.executemany(
                    query.sql(), query.values())
                row_id = cursor.lastrowid
                cursor.close()
                self._connection.commit()
            except:
                self._connection.rollback()
                raise
            return row_id

    @retry(OperationalError)
    def execute_script(self, sql: str):
        if self._connection:
            try:
                cursor = self._connection.executescript(sql)
                cursor.close()
                self._connection.commit()
            except:
                self._connection.rollback()
                raise

    @retry(OperationalError)
    def execute(self, sql_query: str, **kwargs: any) -> any:
        if self._connection:
            try:
                cursor = self._connection.execute(sql_query, kwargs)
                affected_rows = cursor.rowcount
                cursor.close()
                self._connection.commit()
                return affected_rows
            except:
                self._connection.rollback()
                raise

    def test(self) -> bool:
        if self._connection:
            try:
                self._connection.execute("SELECT 1;")
                return True
            except Error:
                return False

    def close(self):
        if self._connection:
            try:
                self._connection.close()
            except Error as ex:
                self.debug(
                    f"Could not gracefully close a connection {self._connection_string}", ex)
