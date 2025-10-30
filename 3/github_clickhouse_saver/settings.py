from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    debug: bool = False

    github_access_token: str

    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="GITHUB_CLICKHOUSE_SAVER_"
    )

settings = Settings()
