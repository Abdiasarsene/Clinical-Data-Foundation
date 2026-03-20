# src/config.py  
from pydantic_settings import BaseSettings, SettingsConfigDict

# ====== CONFIGURATION ======
class Settings(BaseSettings):
    # minIO
    minio_tracking: str
    minio_id: str
    minio_mdp: str
    minio_endpoint: str
    
    # POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str  
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

def get_settings():
    return Settings()

settings = get_settings()