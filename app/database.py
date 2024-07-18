import redis
from dotenv import load_dotenv

import asyncpg
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/dbname')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_DB = os.getenv('REDIS_DB', 1)


class Database:
    def __init__(self):
        self._pool = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(DATABASE_URL)

    async def disconnect(self):
        await self._pool.close()

    async def fetch(self, query: str, *args):
        async with self._pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self._pool.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def execute(self, query: str, *args):
        async with self._pool.acquire() as connection:
            return await connection.execute(query, *args)


class RedisDatabase:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

    def get_user_token(self, user_id):
        return self.r.get(f"{user_id}:session")

    def set_user_token(self, user_id, token):
        return self.r.set(f"{user_id}:session", token)


database = Database()
redis_database = RedisDatabase()