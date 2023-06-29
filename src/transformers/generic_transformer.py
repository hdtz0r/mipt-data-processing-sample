
from typing import Dict, Type
from config.configuration import Configuration
from logging_utils import WithLogger
from models.generic_model import GenericModel
from reflection.dymanic_import_utils import import_class

class GenericTransformer(metaclass=WithLogger):

    _configuration: Configuration = None
    _model_ctor: Type[GenericModel] = None

    def __init__(self, configuration: Configuration) -> None:
        self._configuration = configuration
        model_class = import_class("models", configuration.property("model", "GenericModel"))
        self._model_ctor = model_class

    def transform(self, data: Dict[str, any]) -> GenericModel:
        return self._model_ctor(data)