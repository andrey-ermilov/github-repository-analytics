from db.session import AsyncSessionLocal
from db.repositories import GithubStorage
from api.github_client import AsyncGithubAPIClient
from config import settings
from aiolimiter import AsyncLimiter
import asyncio
import logging 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

limiter = AsyncLimiter(
    max_rate=settings.MAX_RATE, 
    time_period=settings.TIME_PERIOD
)
async def safe(coro):
    async with limiter:
        return await coro

async def init():      
    async with AsyncGithubAPIClient(settings.API_BASE_URL, settings.API_HEADERS) as client:
        logger.info('Searching repositories...')
        repos_full_names = await client.search_repositories( 
            query='language:Python stars:>10', 
            per_page=100, 
            max_pages=3
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
        logger.info(f'Owners fetched: {len(owners_data)}')
        logger.info(f'Repositories fetched: {len(repos_data)}')

        async with AsyncSessionLocal() as session:
            storage = GithubStorage(session, settings.BATCH_SIZE)
            try:
                logger.info('Bulk inserting owners...')
                await storage.bulk_insert_owners(owners_data)
                logger.info('Bulk inserting repositories...')
                await storage.bulk_insert_repositories(repos_data)
                await storage.commit()
                logger.info('Data committed successfully')
            except Exception as e:
                await storage.rollback()
                logger.error(f'Error during DB insert: {e}')
                raise

async def update():
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

if __name__ == '__main__':
    asyncio.run(update())

