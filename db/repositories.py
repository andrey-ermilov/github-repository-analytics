from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from typing import List, Dict, AsyncGenerator
from db.models import (
    Owner,
    Repository,
    OwnerSnapshot,
    RepositorySnapshot,
    TrackedRepository
)


def chunked(iterable: List[dict], size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

class GithubStorage:
    def __init__(self, session, batch_size):
        self.session = session
        self.batch_size = batch_size

    async def _bulk_insert(
        self,
        model,
        rows: List[Dict],
        conflict_column: List[str] = None,
    ):
        for batch in chunked(rows, self.batch_size):
            stmt = insert(model).values(batch)

            if conflict_column:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=conflict_column
                )
            await self.session.execute(stmt)

    async def bulk_insert_owners(self, owners: list[dict]):
        await self._bulk_insert(
            Owner,
            owners,
            conflict_column=['owner_id'],
        )

    async def bulk_insert_repositories(self, repos: List[Dict]):
        await self._bulk_insert(
            Repository,
            repos,
            conflict_column=['repo_id'],
        )

    async def bulk_insert_owner_snapshots(self, snapshots: List[Dict]):
        await self._bulk_insert(
            OwnerSnapshot,
            snapshots,
            conflict_column=['owner_id', 'collected_at'],
        )

    async def bulk_insert_repository_snapshots(self, snapshots: List[Dict]):
        await self._bulk_insert(
            RepositorySnapshot,
            snapshots,
            conflict_column=['repo_id', 'collected_at'],
        )

    async def bulk_insert_tracked_repositories(self, repos: List[Dict]):
        await self._bulk_insert(
            TrackedRepository,
            repos,
            conflict_column=['repo_id'],
        )
    
    async def get_all_tracked_repository_full_names_batch(self) -> AsyncGenerator[List[str], None]:
        offset = 0

        while True:
            stmt = (
                select(Repository.full_name)
                .join(Repository.tracked_info)
            ).limit(self.batch_size)
            
            result = await self.session.execute(stmt)
            batch = result.scalars().all()
            if not batch:
                break
            yield batch
            offset += self.batch_size

    async def get_all_repository_full_names(self) -> List[str]:
        stmt = (
            select(Repository.full_name)
            .join(Repository.tracked_info)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()