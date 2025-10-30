from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    debug: bool = False

    db_user: str
    db_password: str
    db_database: str
    db_host: str
    db_port: int = 5432
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="DB_VERSION_APP_"
    )

settings = Settings()
