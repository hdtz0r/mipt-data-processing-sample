
from datastore.datastore import Datastore
from models.generic_model import GenericModel


class EconomicActivityKindEntry(GenericModel):

    def validate(self) -> bool:
        if self.get("code", None) and self.get("section", None) and self.get("name", None):
            self.setdefault("parent_code", None)
            self.setdefault("comment", None)
            return True
        return False

    def save(self, storage: Datastore):
        insert_sql = f"INSERT INTO hw1 (code, parent_code, section, name, comment) VALUES (:code, :parent_code, :section, :name, :comment);"
        storage.bulk_insert(insert_sql, **self)