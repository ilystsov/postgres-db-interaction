"""
src/homework/clear_tables.py

This module provides a script for clearing tables in a PostgreSQL database.
It is intended to be used in conjunction with the main homework script.

"""

from src.homework.main import create_connection, QueryExecutor
from os import environ, path
from dotenv import load_dotenv

current_directory = path.dirname(path.realpath(__file__))
dotenv_path = path.join(current_directory, "..", "..", "docker", ".env")
load_dotenv(dotenv_path=dotenv_path)


def clear_tables() -> None:
    connection = create_connection(
        db_name=environ.get("POSTGRES_DB"),
        db_user=environ.get("POSTGRES_USER"),
        db_password=environ.get("POSTGRES_PASSWORD"),
        db_host=environ.get("DB_HOST"),
        db_port=environ.get("DB_PORT"),
    )
    if not connection:
        return None
    connection.autocommit = True
    QueryExecutor(connection).execute_query(
        "TRUNCATE TABLE students RESTART IDENTITY CASCADE",
        "clear tables",
    )
    connection.close()
    return None


if __name__ == "__main__":
    clear_tables()
