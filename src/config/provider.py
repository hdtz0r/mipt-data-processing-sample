from yaml import load, dump, Loader
from traceback import format_exc

from config.configuration import Configuration


"""
    Exposes a method to load configuration values from external yaml file
"""


class ConfigurationProvider:

    def load(self, filename: str = "settings.yaml"):
        try:
            with open(f"./{filename}", mode="r+") as stream:
                config: dict = load(stream, Loader=Loader)
                return Configuration(config)
        except:
            print(
                f"Could not load configuration from external file {filename}\n{format_exc()}")
            return self._generate_default(filename)

    def _generate_default(self, filename: str = "settings.yaml"):
        default_confugration = {
            "log": {
                "file-path": "./",
                "level": 20,
                "file-name": "sample-application.log",
                "max-size-in-bytes": 100*1024*8,
                "max-part-size-in-bytes": 1024*1024*8
            },
            "processes": {
                "hw1": {
                    "disabled": False,
                    "datasource": {
                        "data-path": "./data/hw1/",
                        "provider": "FileDataProvider"
                    },
                    "datastore": {
                        "connection-url": "hw1.db",
                        "driver": "SqlLiteDatastore",
                        "parameters": {
                            "init-storage-sql": """
                                CREATE TABLE IF NOT EXISTS hw1 ( 
                                    code TEXT PRIMARY KEY, 
                                    parent_code TEXT, 
                                    section TEXT, 
                                    name TEXT, 
                                    comment TEXT);
                                DELETE FROM hw1;
                            """,
                            "max-pool-size": 1,
                            "batch": {
                                "max-size": 5000
                            },
                            "connection": {
                            }
                        }
                    },
                    "processor": {
                        "max-processes": 1,
                        "max-io-workers": 1,
                        "transformer": {
                            "transformer": "GenericTransformer",
                            "parameters": {
                                "model": "EconomicActivityKindEntry"
                            }
                        }
                    }
                },
                "hw2": {
                    "disabled": False,
                    "datasource": {
                        "data-path": "./data/hw2/",
                        "provider": "FileDataProvider"
                    },
                    "datastore": {
                        "connection-url": "hw1.db",
                        "driver": "SqlLiteDatastore",
                        "parameters": {
                            "init-storage-sql": """
                                CREATE TABLE IF NOT EXISTS telecom_companies ( 
                                    ogrn TEXT PRIMARY KEY, 
                                    inn TEXT, 
                                    kpp TEXT, 
                                    fullname TEXT, 
                                    okvd_code TEXT);
                                DELETE FROM telecom_companies;
                            """,
                            "max-pool-size": 1,
                            "batch": {
                                "max-size": 5000
                            },
                            "connection": {
                            }
                        }
                    },
                    "processor": {
                        "max-processes": 12,
                        "max-io-workers": 4,
                        "transformer": {
                            "transformer": "GenericTransformer",
                            "parameters": {
                                "model": "TelecomCompany"
                            }
                        }
                    }
                }
            }
        }
        try:
            with open(f"./{filename}", "w") as stream:
                dump(default_confugration, stream)
            print(f"Default configuration was generated to {filename}")
            return Configuration(default_confugration)
        except:
            print(
                f"Could not generate default configuration\n{format_exc()}")
            pass


configuration: Configuration = ConfigurationProvider().load()
