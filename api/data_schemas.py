# schemas.py
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class BaseSchema(BaseModel):
    class Config:
        extra = 'ignore'

class OwnerSnapshotSchema(BaseSchema):
    owner_id: int = Field(alias='id')
    collected_at: datetime = Field(default_factory=datetime.now)
    followers: int 
    public_repos: int

class OwnerSchema(BaseModel):
    owner_id: int = Field(alias='id')
    login_name: str = Field(alias='login')
    owner_type: str = Field(alias='type')
    created_at: datetime 
  
class RepositorySnapshotSchema(BaseSchema):
    repo_id: int = Field(alias='id')
    collected_at: datetime = Field(default_factory=datetime.now)
    stars: int = Field(alias='stargazers_count')
    forks: int = Field(alias='forks_count')
    subscribers_count: int
    open_issues: int = Field(alias='has_issues')
    size_kb: int = Field(alias='size')
    pushed_at: datetime

class TrackedRepositorySchema(BaseSchema):
    repo_id: int
    tracking_started_at: datetime
    reason: Optional[str]

class RepositorySchema(BaseSchema):
    repo_id: int = Field(alias='id')
    owner_id: Optional[int] 
    full_name: str
    html_url: str
    repo_language: Optional[str] = Field(alias='language')
    created_at: datetime
    updated_at: datetime
    pushed_at: datetime
    size_kb: int = Field(alias='size')
    is_fork: bool = Field(alias='fork')
    has_issues: bool
    has_projects: bool
    has_downloads: bool
    has_wiki: bool
    has_pages:bool
    has_discussions: bool

    @model_validator(mode='before')
    @classmethod
    def extract_owner_id(cls, values):
        owner = values.get('owner')
        if isinstance(owner, dict):
            values['owner_id'] = owner.get('id')
        return values