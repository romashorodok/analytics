import contextlib
import os
from typing import AsyncIterator, Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def env(name: str, default: str = "") -> str:
    if var := os.environ.get(name):
        return var
    return default


class PostgresDatabaseConfig:
    def __init__(self) -> None:
        self.__provider = env("DB_PROVIDER", "postgresql+asyncpg")
        self.__username = env("DB_USERNAME", "postgres")
        self.__password = env("DB_PASSWORD", "postgres")
        self.__host = env("DB_HOST", "0.0.0.0")
        self.__port = env("DB_PORT", "5432")
        self.__database = env("DB_DATABASE", "postgres")

    def get_uri(self) -> str:
        return "%s://%s:%s@%s:%s/%s" % (
            self.__provider,
            self.__username,
            self.__password,
            self.__host,
            self.__port,
            self.__database,
        )


class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}) -> None:
        self.__engine: AsyncEngine | None = create_async_engine(host, **engine_kwargs)
        self.__session_maker: async_sessionmaker[AsyncSession] | None = (
            async_sessionmaker(self.__engine, autoflush=False, expire_on_commit=False)
        )

    async def close(self):
        if not self.__engine:
            raise Exception("DatabaseSessionManager is not initialized")
        await self.__engine.dispose()

        self.__engine = None
        self.__session_maker = None

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if not self.__engine or not self.__session_maker:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self.__session_maker()
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

    def session_maker(self) -> async_sessionmaker[AsyncSession]:
        if not self.__engine or not self.__session_maker:
            raise Exception("DatabaseSessionManager is not initialized")
        return self.__session_maker
