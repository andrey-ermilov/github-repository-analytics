import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    GITHUB_TOKEN: str 
    API_BASE_URL: str  
    
    DB_HOST: str 
    DB_PORT: str 
    DB_NAME: str 
    DB_USER: str 
    DB_PASSWORD: str 
    
    BATCH_SIZE: int 
    REQUEST_DELAY: float 
    
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    )

    @property
    def API_HEADERS(self) -> dict:
        return {
            "Authorization": f"token {self.GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }

    @property
    def DB_URL(self):
        return (f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@"
                f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")
       
settings = Settings()