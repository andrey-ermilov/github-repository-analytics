from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, Integer, Boolean, String, ForeignKey, Text, DateTime, Index
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs
from typing import Optional, List
from datetime import datetime

class Base(AsyncAttrs, DeclarativeBase):
    __abstract__ = True  

class Owner(Base):
    __tablename__ = 'owners'

    owner_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    login_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    owner_type: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    snapshots: Mapped[List['OwnerSnapshot']] = relationship(
        back_populates='owner',
        cascade='save-update, merge', 
        passive_deletes=True
    )
    repositories: Mapped[List['Repository']] = relationship(
        back_populates='owner'
    )

    __table_args__ = (
        Index('ix_owner_type', 'owner_type'),
        Index('ix_owner_created', 'created_at')
    )

class OwnerSnapshot(Base):
    __tablename__ = 'owners_snapshots'

    owner_id: Mapped[int] = mapped_column(
        BigInteger, 
        ForeignKey('owners.owner_id', ondelete='CASCADE'), 
        primary_key=True
    )
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    followers: Mapped[int] = mapped_column(Integer, nullable=False)
    public_repos: Mapped[int] = mapped_column(Integer, nullable=False)

    owner: Mapped['Owner'] = relationship(
        back_populates='snapshots'
    )

    __table_args__ = (
        Index('ix_owner_snapshots_collected', 'owner_id', 'collected_at'),
        Index('ix_owner_snapshots_date', 'collected_at'),
    )

class Repository(Base):
    __tablename__ = 'repositories'

    repo_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    owner_id: Mapped[int] = mapped_column(
        BigInteger, 
        ForeignKey('owners.owner_id', ondelete='SET NULL'), 
        nullable=True
    )
    full_name: Mapped[str] = mapped_column(String(300), unique=True)
    html_url: Mapped[str] = mapped_column(String(500))
    repo_language: Mapped[Optional[str]] = mapped_column(String(50))  
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pushed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    size_kb: Mapped[int] = mapped_column(Integer, nullable=False)
    is_fork: Mapped[bool] = mapped_column(Boolean, nullable=False)
    has_issues: Mapped[bool] = mapped_column(Boolean, nullable=False)
    has_wiki: Mapped[bool] = mapped_column(Boolean, nullable=False)

    owner: Mapped[Optional['Owner']] = relationship(
        back_populates='repositories'
    )
    snapshots: Mapped[List['RepositorySnapshot']] = relationship(
        back_populates='repository',
        cascade='all, delete-orphan'
    )
    tracked_info: Mapped[Optional['TrackedRepository']] = relationship(
        back_populates='repository', 
        uselist=False, 
        cascade='all, delete-orphan'
    )


    __table_args__ = (
        Index('ix_repo_language', 'repo_language'),
        Index('ix_repo_created', 'created_at'),
        Index('ix_repo_owner', 'owner_id'),
    )

class RepositorySnapshot(Base):
    __tablename__ = 'repositories_snapshots'

    repo_id: Mapped[int] = mapped_column(
        BigInteger, 
        ForeignKey('repositories.repo_id', ondelete='CASCADE'), 
        primary_key=True
    )
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    stars: Mapped[int] = mapped_column(Integer, nullable=False)
    forks: Mapped[int] = mapped_column(Integer, nullable=False)
    watchers: Mapped[int] = mapped_column(Integer, nullable=False)
    open_issues: Mapped[int] = mapped_column(Integer, nullable=False)
    size_kb: Mapped[int] = mapped_column(Integer, nullable=False)
    pushed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    repository: Mapped['Repository'] = relationship(
        back_populates='snapshots'
    )

    __table_args__ = (
        Index('ix_repo_snapshots_collected', 'collected_at'),
        Index('ix_repo_snapshots_repo_collected', 'repo_id', 'collected_at'),
        Index('ix_repo_snapshots_stars', 'stars'),
        Index('ix_repo_snapshots_forks', 'forks'),
    )

class TrackedRepository(Base):
    __tablename__ = 'tracked_repositories'

    repo_id: Mapped[int] = mapped_column(BigInteger,  ForeignKey('repositories.repo_id', ondelete='CASCADE'), primary_key=True)
    tracking_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str] = mapped_column(Text)

    repository: Mapped['Repository'] = relationship(
        back_populates='tracked_info', 
        uselist=False,
        single_parent=True
    )

    __table_args__ = (
        Index('ix_tracked_started', 'tracking_started_at'),
    )