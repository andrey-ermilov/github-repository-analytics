from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from typing import List, Dict
from db.models import (
    Owner,
    Repository,
    OwnerSnapshot,
    RepositorySnapshot,
    TrackedRepository
)


class GithubStorage:
    def __init__(self, session):
        self.session = session

    async def bulk_insert_owners(self, owners: List[Dict]):
        stmt = insert(Owner).values(owners)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['owner_id']
        )
        await self.session.execute(stmt)

    async def bulk_insert_repositories(self, repos: List[Dict]):
        stmt = insert(Repository).values(repos)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['repo_id']
        )
        await self.session.execute(stmt)


    async def bulk_insert_owner_snapshots(self, snapshots: List[Dict]):
        stmt = insert(OwnerSnapshot).values(snapshots)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['owner_id', 'collected_at']
        )
        await self.session.execute(stmt)

    async def bulk_insert_repository_snapshots(self, snapshots: List[Dict]):
        stmt = insert(RepositorySnapshot).values(snapshots)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['repo_id', 'collected_at']
        )
        await self.session.execute(stmt)

    async def bulk_insert_tracked_repositories(self, repos: List[Dict]):
        stmt = insert(TrackedRepository).values(repos)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['repo_id']
        )
        await self.session.execute(stmt)
    
    async def get_all_repository_full_names(self) -> List[str]:
        stmt = select(Repository.full_name)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()

    async def close(self):
        if self.session:
            await self.session.close()