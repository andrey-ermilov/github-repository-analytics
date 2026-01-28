import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

import httpx
from typing import List, Optional, Any
from api.data_schemas import *
import logging

logger = logging.getLogger('httpx')
logger.setLevel(logging.WARNING)

logger.propagate = False

file_handler = logging.FileHandler('pipeline.log')
file_handler.setLevel(logging.WARNING)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)


formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

class AsyncGithubAPIClient:
    def __init__(self, base_url: str, headers: dict):
        self.base_url = base_url
        self.headers = headers
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            timeout=httpx.Timeout(10.0, connect=3.0),
        )

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            timeout=httpx.Timeout(10.0, connect=3.0),
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def _make_request(self, url: str, params: dict = None) -> Optional[Any]:
        try:
            response = await self.client.get(
                url=self.base_url + url,
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            logger.warning(f'HTTP error for {url}: {e.response.status_code} {e.response.text}')
            if e.response.status_code == 403:
                raise
            return None

        except httpx.RequestError as e:
            logger.error(f'Request failed for {url}: {str(e)}')
            return None
        
    async def _fetch(self, endpoint: str, schema):
        data = await self._make_request(endpoint)
        if not data:
            return None
        return schema(**data)

    async def search_repositories(self, query: str, sort: str = 'stars', 
                           order: str = 'desc', per_page: int = 100, 
                           max_pages: int = 5) -> List[str]:
        all_repos = []
        
        for page in range(1, max_pages + 1):
            url = '/search/repositories'
            params = {
                'q': query,
                'sort': sort,
                'order': order,
                'per_page': per_page,
                'page': page
            }
            data = await self._make_request(url, params)
            if not data or 'items' not in data:
                continue
            for repo in data['items']: 
                all_repos.append(repo['full_name'])
            if len(data['items']) < per_page:
                break

        return all_repos
    
    async def fetch_repository(self, owner: str, repo: str) -> Optional[RepositorySchema]:
        endpoint = f'/repos/{owner}/{repo}'
        return await self._fetch(endpoint, RepositorySchema)

    async def fetch_owner(self, owner: str) -> Optional[OwnerSnapshotSchema]: 
        endpoint = f'/users/{owner}'
        return await self._fetch(endpoint, OwnerSchema)
    
    async def fetch_repository_snapshot(self, owner: str, repo: str) -> Optional[RepositorySnapshotSchema]:
        endpoint = f'/repos/{owner}/{repo}'
        return await self._fetch(endpoint, RepositorySnapshotSchema)

    async def fetch_owner_snapshot(self, owner: str) -> Optional[OwnerSnapshotSchema]:
        endpoint = f'/users/{owner}'
        return await self._fetch(endpoint, OwnerSnapshotSchema)