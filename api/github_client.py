import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

import requests
import time
from typing import List, Dict, Optional
import logging
from datetime import datetime, timedelta
from config import settings

logger = logging.getLogger(__name__)

class GithubAPIClient:
    def __init__(self, base_url: str, headers: dict):
        self.base_url = base_url
        self.headers = headers
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(self, url: str, params: dict = None) -> Optional[Dict]:
        try:
            response = self.session.get(
                url=self.base_url + url,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            # check limits
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed for {url}: {e}')
            return None

    def search_repositories(self, query: str, sort: str = 'stars', # raw data, fill repositories, tracked_repositories, owners
                           order: str = 'desc', per_page: int = 100, 
                           max_pages: int = 5) -> List[Dict]:
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
                break
            repos = data['items'] 
            all_repos.extend(repos)
            logger.info(f'Page {page}: fetched {len(repos)} repositories')
            
            if len(repos) < per_page:
                break

        logger.info(f'Total fetched: {len(all_repos)} repositories')
        return all_repos
    
    def get_repository_info(): # fill repositories_snapshots
        pass

    def get_owner_info(): # fill owners_snapshots
        pass

    def get_trending_repos(self, days: int = 30, min_stars: int = 100) -> List[Dict]: # for test
        since_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        query = f'created:>{since_date} stars:>={min_stars}'
        
        return self.search_repositories(
            query=query,
            sort='stars',
            order='desc',
            per_page=100,
            max_pages=3
        )
    
gac = GithubAPIClient(settings.API_BASE_URL, settings.API_HEADERS)
print(gac.get_trending_repos()[-1])