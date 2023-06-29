
import cProfile
from logging import Logger, getLogger, Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from sys import stdout
from time import time
from typing import Callable, Self, Type, cast
from config.provider import configuration

"""
    Exposes a set of methods to log records using 
    configured Logger instance
"""


class WithLogger(type):

    _instances = {}

    def __new__(cls, classname, superclasses, attributedict) -> Self:
        cls._instances.setdefault(
            classname, _bootstrap_logger(f"{classname}.class"))
        logger_instance = cls._instances.get(classname)
        attributedict.update({
            "_logger": logger_instance,
            "info": WithLogger.info,
            "warn": WithLogger.warn,
            "debug": WithLogger.debug,
            "error": WithLogger.error,
            "panic": WithLogger.panic,
            "getLogger": WithLogger.getLogger
        })
        return super().__new__(cls, classname, superclasses, attributedict)

    @staticmethod
    def info(self, msg: str):
        self._logger.info(msg)

    @staticmethod
    def warn(self, msg: str):
        self._logger.warning(msg)

    @staticmethod
    def debug(self, msg: str):
        self._logger.debug(msg)

    @staticmethod
    def error(self, msg: str, ex: Exception = None):
        self._logger.error(msg, exc_info=ex, stack_info=True)

    @staticmethod
    def panic(self, msg: str, ex: Exception = None):
        self._logger.critical(msg, exc_info=ex, stack_info=True)

    @staticmethod
    def getLogger(self) -> Logger:
        return self._logger


"""
    Decorates the specified class to enrich with logging methods
"""


def uselog_deprecated(cls: type) -> Type:

    cls_wrapper = type(f"LoggingWrapper${cls.__name__}", (WithLogger, cls), {
        "_logger": _bootstrap_logger(f"{cls.__qualname__}\{cls.__name__}.class"),
        "_base_class": cls
    })

    return cast(Type[cls], cls_wrapper)


def time_and_log(method: Callable):
    def wrapper(instance, *args, **kwargs):
        current_ts = time()
        result = method(instance, *args, **kwargs)
        tooks = time() - current_ts
        if hasattr(instance, "getLogger"):
            logger: Logger = instance.getLogger()
            logger.info(
                f"Method invokation {instance.__class__.__name__}->{method.__name__} tooks {tooks} seconds")
        return result
    return wrapper


def profile(method: Callable):
    def wrapper(*args: any, **kwargs: any):
        profile_filename = f"{method.__name__}.prof"
        if args and len(args) > 0 and hasattr(args[0], "__class__"):
            profile_filename = f"{args[0].__class__.__name__}.{method.__name__}.prof"
        profiler = cProfile.Profile()
        result = profiler.runcall(method, *args, **kwargs)
        profiler.dump_stats(profile_filename)
        return result
    return wrapper


def _bootstrap_logger(name: str) -> Logger:
    logger = getLogger(name)
    logger.setLevel(configuration.property("log.level", 20))
    logger.addHandler(default_log_handler)
    logger.addHandler(default_log_file_handler)
    return logger


default_log_formatter: Formatter = Formatter(
    fmt="%(asctime)s: [%(levelname)s] [%(processName)-10s] [%(threadName)s] [%(name)s]\t-\t%(message)s")
default_log_handler: StreamHandler = StreamHandler(stream=stdout)
default_log_handler.setFormatter(default_log_formatter)

log_max_part_size_in_bytes = configuration.property(
    "log.max-part-size-in-bytes", 100*1024*8)
log_max_size_in_bytes = configuration.property(
    "log.max-size-in-bytes", 1024*1024*8)
log_file_path = configuration.property("log.file-path", "./")
log_file_name = configuration.property("log.file-name", "application.log")

default_log_file_handler: StreamHandler = RotatingFileHandler(filename=f'{log_file_path}/{log_file_name}',
                                                              mode="w", maxBytes=log_max_part_size_in_bytes,
                                                              backupCount=round(log_max_size_in_bytes/log_max_part_size_in_bytes))
default_log_file_handler.setFormatter(default_log_formatter)
