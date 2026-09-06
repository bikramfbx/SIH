"""Small DB helper for the API. No credentials are hardcoded; everything
comes from environment variables (POSTGRES_*), same as the ingest scripts."""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


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