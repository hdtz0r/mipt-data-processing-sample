
from datastore.datastore import Datastore
from models.generic_model import GenericModel


class TelecomCompany(GenericModel):

    def validate(self) -> bool:
        if self.get("ogrn") and self.get("data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД") and (self.get("full_name") or self.get("name")):
            return True
        return super().validate()

    def filter(self) -> bool:
        okvd_code = self.get(
            "data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД")
        if okvd_code and str(okvd_code).startswith("61"):
            return True
        return False

    def save(self, storage: Datastore):
        insert_sql = f"INSERT INTO telecom_companies (ogrn, inn, kpp, fullname, okvd_code) VALUES (:ogrn, :inn, :kpp, :company_name, :okvd_code);"
        parameters = {
            "okvd_code": self.get("data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД"),
            "company_name": self.get("full_name", self.get("name")),
            "ogrn": self.get("ogrn"),
            "inn": self.get("inn"),
            "kpp": self.get("kpp"),
        }
        storage.bulk_insert(insert_sql, **parameters)
