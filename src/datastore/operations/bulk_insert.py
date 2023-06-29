
from typing import Dict, List


class BulkInsert:

    _sql: str
    _limit: int
    _values: List[Dict[str, any]]

    def __init__(self, sql: str, batch_size: int) -> None:
        self._limit = batch_size
        self._sql = sql
        self._values = []

    def sql(self) -> str:
        return self._sql
    
    def store(self, values: Dict[str,any]):
        self._values.append(values)

    def values(self):
        for value in self._values:
            yield value

    def is_completed(self) -> bool:
        return len(self._values) >= self._limit