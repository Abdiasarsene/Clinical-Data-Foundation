# src/config.py  
from pydantic_settings import BaseSettings, SettingsConfigDict

# ====== CONFIGURATION ======
class Settings(BaseSettings):
    # minIO
    minio_tracking: str
    minio_id: str
    minio_mdp: str
    minio_endpoint: str
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )

settings = Settings()