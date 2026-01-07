from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9094"
    KAFKA_TOPIC: str = "test-topic"
    KAFKA_GROUP_ID: str = "test-group"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
