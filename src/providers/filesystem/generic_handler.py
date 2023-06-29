

from typing import List

from logging_utils import WithLogger


class GenericHandler(metaclass=WithLogger):

    def get_ext(self) -> List[str]:
        return []
    
    def cleanup(self):
        pass