"""Small DB helper for the API. No credentials are hardcoded; everything
comes from environment variables (POSTGRES_*), same as the ingest scripts."""

import os

import psycopg
from dotenv import load_dotenv

try:
    load_dotenv()
except Exception:
    # python-dotenv can raise under some Python versions when invoked at odd
    # call depths; env vars are also injected by docker-compose, so a failure
    # to find a local .env is not fatal.
    pass


def connection_params():
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }


def connect():
    return psycopg.connect(**connection_params())