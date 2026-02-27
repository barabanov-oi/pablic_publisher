import os


class BaseConfig:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-key")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///publisher.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "connect_args": {
            "timeout": int(os.getenv("SQLITE_TIMEOUT_SECONDS", "30")),
        }
    }

    MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
    DEFAULT_RETRY_MINUTES = int(os.getenv("DEFAULT_RETRY_MINUTES", "30"))
    WORKER_INTERVAL_SECONDS = int(os.getenv("WORKER_INTERVAL_SECONDS", "20"))
    PROCESSING_TTL_SECONDS = int(os.getenv("PROCESSING_TTL_SECONDS", "900"))
    DISABLE_SCHEDULER = os.getenv("DISABLE_SCHEDULER", "0") == "1"


class DevelopmentConfig(BaseConfig):
    DEBUG = True


class ProductionConfig(BaseConfig):
    DEBUG = False


CONFIG_MAP = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
}
