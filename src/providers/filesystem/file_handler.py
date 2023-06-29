
from typing import List
from providers.datasource import Datasource
from providers.filesystem.archive_content_loader import ArchiveContentLoader
from providers.filesystem.generic_handler import GenericHandler


class FileHandler(GenericHandler):

    def load(self, filename: str, archive_file_loader: ArchiveContentLoader = None) -> List[Datasource]:
        pass
