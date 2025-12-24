import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

import requests
from typing import List, Dict, Optional, Any
import logging
from data_schemas import *


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('api/github_api.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class GithubAPIClient:
    def __init__(self, base_url: str, headers: dict):
        self.base_url = base_url
        self.headers = headers
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(self, url: str, params: dict = None) -> Optional[Any]:
        try:
            response = self.session.get(
                url=self.base_url + url,
                params=params,
                timeout=(3, 10)
            )
            response.raise_for_status()
            remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            if remaining < 10:
                logger.warning(f'Low API rate limit: {remaining} requests remaining')

            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed for {url}: {e}')
            return None
        
    def _fetch(self, endpoint: str, schema):
        data = self._make_request(endpoint)
        if not data:
            return None
        return schema(**data)

    def search_repositories(self, query: str, sort: str = 'stars', 
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
            data = self._make_request(url, params)
            if not data or 'items' not in data:
                continue
            for repo in data['items']: 
                all_repos.append(repo['full_name'])
            logger.info(f'Page {page}: fetched {len(data['items'])} repositories')
            if len(data['items']) < per_page:
                break
        logger.info(f'Total fetched: {len(all_repos)} repositories')

        return all_repos
    
    def fetch_repository(self, owner: str, repo: str) -> Optional[RepositorySchema]:
        endpoint = f'/repos/{owner}/{repo}'
        return self._fetch(endpoint, RepositorySchema)

    def fetch_owner(self, owner: str) -> Optional[OwnerSnapshotSchema]: 
        endpoint = f'/users/{owner}'
        return self._fetch(endpoint, OwnerSchema)
    
    def fetch_repository_snapshot(self, owner: str, repo: str) -> Optional[RepositorySnapshotSchema]:
        endpoint = f'/repos/{owner}/{repo}'
        return self._fetch(endpoint, RepositorySnapshotSchema)

    def fetch_owner_snapshot(self, owner: str) -> Optional[OwnerSnapshotSchema]:
        endpoint = f'/users/{owner}'
        return self._fetch(endpoint, OwnerSnapshotSchema)
    
    def close(self):
        if self.session:
            self.session.close()
    