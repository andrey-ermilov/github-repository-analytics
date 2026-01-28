from db.session import AsyncSessionLocal
from db.repositories import GithubStorage
from api.github_client import AsyncGithubAPIClient
from config import settings
from aiolimiter import AsyncLimiter
import asyncio
import logging 
from datetime import datetime
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('pipeline')

list_init_params = [
    {
        'name': 'python_mid_popular',
        'query': 'language:python stars:200..1000 pushed:>2024-01-01',
        'per_page': 50,
        'max_pages': 6,   
    },
    {
        'name': 'python_fast_growing',
        'query': 'language:python stars:50..200 pushed:>2024-03-01',
        'per_page': 50,
        'max_pages': 4,  
    },
    {
        'name': 'python_top_ecosystem',
        'query': 'language:python stars:>5000',
        'per_page': 30,
        'max_pages': 2,   
    },
    {
        'name': 'js_mid_popular',
        'query': 'language:javascript stars:200..1000 pushed:>2024-01-01 -language:typescript',
        'per_page': 50,
        'max_pages': 8,   
    },
    {
        'name': 'go_mid_popular',
        'query': 'language:go stars:100..800 pushed:>2024-01-01',
        'per_page': 50,
        'max_pages': 5,   
    },
    {
        'name': 'go_fast_growing',
        'query': 'language:go stars:50..150 pushed:>2024-03-01',
        'per_page': 50,
        'max_pages': 2,   
    },
    {
        'name': 'rust_mid_popular',
        'query': 'language:rust stars:300..1500 pushed:>2024-01-01',
        'per_page': 50,
        'max_pages': 3,  
    },
    {
        'name': 'rust_fast_growing',
        'query': 'language:rust stars:50..300 pushed:>2024-03-01',
        'per_page': 50,
        'max_pages': 3,   
    },
    {
        'name': 'java_mid_popular',
        'query': 'language:java stars:300..2000 pushed:>2024-01-01',
        'per_page': 50,
        'max_pages': 6,   
    },
    {
        'name': 'java_top_legacy',
        'query': 'language:java stars:>3000',
        'per_page': 30,
        'max_pages': 2,   
    }
]

async def init(params):      
    limiter = AsyncLimiter(
        max_rate=settings.MAX_RATE, 
        time_period=settings.TIME_PERIOD
    )
    async def safe(coro):
        async with limiter:
            return await coro

    async with AsyncGithubAPIClient(settings.API_BASE_URL, settings.API_HEADERS) as client:
        logger.info(f'Searching repositories {params['name']}...')
        repos_full_names = await client.search_repositories( 
            query=params['query'], 
            per_page=params['per_page'], 
            max_pages=params['max_pages']
        )
        logger.info(f'Found {len(repos_full_names)} repositories')

        owner_repo_pairs = [
            tuple(full_name.split('/'))
            for full_name in repos_full_names
        ]
        unique_owners = {owner for owner, _ in owner_repo_pairs}
        logger.info(f'Unique owners to fetch: {len(unique_owners)}')

        logger.info('Fetching owners...')
        owners = await asyncio.gather(
            *(safe(client.fetch_owner(owner)) for owner in unique_owners)
        )
        logger.info('Fetching repositories...')
        repos = await asyncio.gather(
            *(safe(client.fetch_repository(owner, repo)) for owner, repo in owner_repo_pairs)
        )

        owners_data = [
            o.model_dump(by_alias=False)
            for o in owners if o is not None
        ]
        repos_data = [
            r.model_dump(by_alias=False)
            for r in repos if r is not None
        ]

        for repo in repos_data:
            if 'repo_id' not in repo:
                logger.error(f"Missing repo_id: {repo}")
        tracked = [
            {
                'repo_id': repo['repo_id'],
                'tracking_started_at': datetime.now(),
                'reason': params['name'],
            }
            for repo in repos_data
        ]       

        logger.info(f'Owners fetched: {len(owners_data)}')
        logger.info(f'Repositories fetched: {len(repos_data)}')

        async with AsyncSessionLocal() as session:
            storage = GithubStorage(session, settings.BATCH_SIZE)
            try:
                if owners_data:
                    logger.info('Bulk inserting owners...')
                    await storage.bulk_insert_owners(owners_data)
                if repos_data:
                    logger.info('Bulk inserting repositories...')
                    await storage.bulk_insert_repositories(repos_data)
                if tracked:
                    logger.info('Bulk inserting tracked...')
                    await storage.bulk_insert_tracked_repositories(tracked)
                await storage.commit()
                logger.info('Data committed successfully')
            except Exception as e:
                await storage.rollback()
                logger.error(f'Error during DB: {e}')
                raise

async def update():
    limiter = AsyncLimiter(
        max_rate=settings.MAX_RATE, 
        time_period=settings.TIME_PERIOD
    )
    async def safe(coro):
        async with limiter:
            return await coro

    async with AsyncSessionLocal() as session:
        storage = GithubStorage(session, settings.BATCH_SIZE)
        try:
            logger.info('Starting to fetch repository full names...')
            full_names = await storage.get_all_repository_full_names()
            owner_repo_pairs = [
                tuple(full_name.split('/'))
                for full_name in full_names
            ]
            unique_owners = {owner for owner, _ in owner_repo_pairs}
        except Exception as e:
            await storage.rollback()
            logger.error(f'Error during DB: {e}')
            raise
        
    async with AsyncGithubAPIClient(settings.API_BASE_URL, settings.API_HEADERS) as client:
        logger.info('Fetching owners snapshots...')
        owners = await asyncio.gather(
            *(safe(client.fetch_owner_snapshot(owner)) for owner in unique_owners)
        )
        logger.info('Fetching repositories snapshots...')
        repos = await asyncio.gather(
            *(safe(client.fetch_repository_snapshot(owner, repo)) for owner, repo in owner_repo_pairs)
        )

    owners_snapshots = [
        o.model_dump(by_alias=False)
        for o in owners if o is not None
    ]
    repos_snapshots = [
        r.model_dump(by_alias=False)
        for r in repos if r is not None
    ]

    async with AsyncSessionLocal() as session:
        storage = GithubStorage(session, settings.BATCH_SIZE)
        try:
            logger.info('Bulk inserting owners snapshots...')
            await storage.bulk_insert_owner_snapshots(owners_snapshots)
            logger.info('Bulk inserting repositories snapshots...')
            await storage.bulk_insert_repository_snapshots(repos_snapshots)
            await storage.commit()
            logger.info('Data committed successfully')
        except Exception as e:
            await storage.rollback()
            logger.error(f'Error during DB: {e}')
            raise

def parse_args():
    parser = argparse.ArgumentParser(
        description='GitHub analytics data pipeline'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--init',
        action='store_true',
        help='Initial load of repositories, owners and tracked repositories'
    )
    group.add_argument(
        '--update',
        action='store_true',
        help='Update snapshots for tracked repositories, and owners'
    )

    return parser.parse_args()

async def main():
    args = parse_args()

    if args.init:
        for params in list_init_params:
            await init(params)
    elif args.update:
        await update()

if __name__ == '__main__':
    asyncio.run(main())