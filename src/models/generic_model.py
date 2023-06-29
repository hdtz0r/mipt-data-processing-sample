from typing import Dict
from datastore.datastore import Datastore
from logging_utils import WithLogger
from models.data_container import DataContainer

"""
    Represents a generic data model
"""

class GenericModel(DataContainer, metaclass=WithLogger):

    def __init__(self, data: Dict[str, any]) -> None:
        for k, v in data.items():
            self.__setitem__(k, v)

    def save(self, storage: Datastore):
        pass

    def validate(self) -> bool:
        return False
    
    def filter(self) -> bool:
        return True
