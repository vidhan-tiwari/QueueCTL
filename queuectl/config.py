from queuectl.db import get_config, set_config

class Config:
    @staticmethod
    def get_max_retries() -> int:
        return int(get_config("max_retries", "3"))

    @staticmethod
    def set_max_retries(val: int):
        set_config("max_retries", str(val))

    @staticmethod
    def get_backoff_base() -> float:
        return float(get_config("backoff_base", "2.0"))

    @staticmethod
    def set_backoff_base(val: float):
        set_config("backoff_base", str(val))

    @staticmethod
    def get_default_timeout() -> int:
        return int(get_config("default_timeout", "300"))

    @staticmethod
    def set_default_timeout(val: int):
        set_config("default_timeout", str(val))
