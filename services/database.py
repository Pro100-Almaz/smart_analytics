import redis
from dotenv import load_dotenv

import psycopg2
from psycopg2 import sql

import os

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_DB = os.getenv('REDIS_DB', 0)

DB_NAME = os.getenv('DB_NAME', "smart_analytics")
DB_USER = os.getenv('DB_USER', "root")
DB_PASSWORD = os.getenv('DB_PASSWORD', "1234")
DB_HOST = os.getenv('DB_HOST', "localhost")


class Database:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        self.cursor.close()
        self.conn.close()

    def execute(self, query: str, *args):
        self.cursor.execute(query, *args)
        self.conn.commit()

    def execute_with_return(self, query: str, *args):
        self.cursor.execute(query, *args)
        return_value = self.cursor.fetchall()
        self.conn.commit()

        return return_value


database = Database()
redis_database = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)