"""
src/homework/main.py

This module contains a script for interacting with a PostgreSQL
database, specifically designed for storing information about students
and their exam scores. The script addresses the tasks outlined in AB
homework 10, covering table creation, data insertion, data updating,
data deletion, data querying, exception handling, and additional features.

"""

from os import environ, path
from typing import Sequence
from psycopg2 import (
    OperationalError,
    ProgrammingError,
    IntegrityError,
    extensions,
    connect,
    sql,
)
from dotenv import load_dotenv
from faker import Faker
from prettytable import PrettyTable

ROW_LIMIT = 30
SEED = 1
Faker.seed(SEED)
faker = Faker()

current_directory = path.dirname(path.realpath(__file__))
dotenv_path = path.join(current_directory, "..", "..", "docker", ".env")
load_dotenv(dotenv_path=dotenv_path)


def create_connection(
    db_name: str | None,
    db_user: str | None,
    db_password: str | None,
    db_host: str | None,
    db_port: str | None,
) -> None | extensions.connection:
    if None in (db_name, db_user, db_password, db_host, db_port):
        return None
    try:
        connection = connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except (OperationalError, ProgrammingError) as e:
        print(f'Failed to create connection: error "{e}" occurred.')
        return None
    print("Connection to PostgreSQL DB successful!")
    return connection


def print_table(data: list | None, column_names: list) -> None:
    if not data:
        return
    try:
        table = PrettyTable(column_names)
        for row in data:
            table.add_row(row)
    except ValueError as e:
        print(f'Failed to print table: error "{e}" occurred.')
        return
    print(table)


class QueryExecutor:
    def __init__(self, connection: extensions.connection) -> None:
        self.connection = connection

    def execute_query(
        self,
        query: str | sql.SQL | sql.Composed,
        query_name: str,
        query_params: Sequence | None = None,
        raise_exc: bool = False,
        print_success: bool = True,
    ) -> None:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, query_params)
                if print_success:
                    print(f'Query "{query_name}" executed successfully!')
        except (OperationalError, ProgrammingError, IntegrityError) as e:
            print(f'Failed to {query_name}: error "{e}" occurred.')
            if raise_exc:
                raise

    def execute_query_return_cursor(
        self,
        query: str | sql.SQL | sql.Composed,
        query_name: str,
        query_params: Sequence | None = None,
        raise_exc: bool = False,
        print_success: bool = True,
    ) -> extensions.cursor | None:
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, query_params)
            if print_success:
                print(f'Query "{query_name}" executed successfully!')
            return cursor
        except (OperationalError, ProgrammingError, IntegrityError) as e:
            print(f'Failed to {query_name}: error "{e}" occurred.')
            if raise_exc:
                if cursor is not None:
                    cursor.close()
                raise
            return None


class CreateTable(QueryExecutor):
    def create_students_table(self) -> None:
        query = """
            CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            birth_date DATE NOT NULL ,
            "group" INTEGER NOT NULL
            )
            """
        self.execute_query(query, "create students table", raise_exc=True)

    def create_scores_table(self) -> None:
        query = """
            CREATE TABLE IF NOT EXISTS scores (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            student_id INT REFERENCES students(id) ON DELETE CASCADE NOT NULL,
            score INTEGER NOT NULL
            )
            """
        self.execute_query(query, "create scores table", raise_exc=True)


class InsertData(QueryExecutor):
    def insert_students(self, students_number: int = 10) -> None:
        insert_query = """
            INSERT INTO students (name, birth_date, "group")
            VALUES (%s, %s, %s)
            """
        for _ in range(students_number):
            self.execute_query(
                insert_query,
                "insert students",
                (faker.name(), faker.date_of_birth(), faker.random_int(1, 10)),
                True,
                False,
            )
        print("Students table filled successfully!")

    def insert_scores(self) -> None:
        insert_query = """
            INSERT INTO scores (student_id, score)
            VALUES (%s, %s)
            """
        cursor = self.execute_query_return_cursor(
            "SELECT id FROM students", "select students' identifiers"
        )
        if cursor:
            rows = cursor.fetchall()
            cursor.close()
            for row in rows:
                student_id = row[0]
                self.execute_query(
                    insert_query,
                    "insert scores",
                    (student_id, faker.random_int(0, 100)),
                    True,
                    False,
                )
            print("Scores table filled successfully!")


class UpdateData(QueryExecutor):
    def update_group(self, student_id: int, new_group: int) -> None:
        update_query = """
        UPDATE students SET "group" = %s WHERE id = %s
        """
        cursor = self.execute_query_return_cursor(
            update_query, "update student's group", (new_group, student_id)
        )
        if cursor:
            changes_number = cursor.rowcount
            cursor.close()
            if changes_number == 0:
                print(f"No student with id {student_id} found.")
            else:
                print(
                    f"Student with id {student_id} now in group {new_group}."
                )


class DeleteData(QueryExecutor):
    def delete_group(self, group: int) -> None:
        delete_query = """
        DELETE FROM students WHERE "group" = %s
        """
        cursor = self.execute_query_return_cursor(
            delete_query, "delete group", (group,)
        )
        if cursor:
            changes_number = cursor.rowcount
            cursor.close()
            if changes_number == 0:
                print(f"No student from group {group} found.")
            else:
                print(f"{changes_number} student(s) of group {group} deleted.")


class QueryData(QueryExecutor):
    def fetch_group(self, group: int) -> list | None:
        select_query = """
        SELECT id, name, birth_date FROM students WHERE "group" = %s LIMIT %s
        """
        cursor = self.execute_query_return_cursor(
            select_query, "fetch group", (group, ROW_LIMIT)
        )
        if cursor:
            result = cursor.fetchall()
            if result:
                print(f"Group number {group} fetched!")
            cursor.close()
            return result
        return None


class ComplexQuery(QueryExecutor):
    def calculate_group_average_score(self, group: int) -> None:
        calculate_query = """
        SELECT AVG(scores.score)
        FROM students
        JOIN scores ON students.id = scores.student_id
        WHERE students."group" = %s
        """
        cursor = self.execute_query_return_cursor(
            calculate_query, "calculate group's average score", (group,)
        )
        if cursor:
            result = cursor.fetchone()
            cursor.close()
            if result is not None and result[0] is not None:
                score = result[0]
                print(f"Group's number {group} average score: {score:.2f}")
            else:
                print(f"No group number {group} in the DB.")


class AdditionalFeatures(QueryExecutor):
    def fetch_sorted_data(self, table: str, column: str) -> list | None:
        select_query = sql.SQL(
            "SELECT * FROM {table} ORDER BY {column} LIMIT %s"
        ).format(table=sql.Identifier(table), column=sql.Identifier(column))
        cursor = self.execute_query_return_cursor(
            select_query,
            f"fetch data for {table} table sorted by {column}",
            (ROW_LIMIT,),
        )
        if cursor:
            result = cursor.fetchall()
            cursor.close()
            return result
        return None

    def search_student_by_name(self, partial_name: str) -> list | None:
        select_query = """
        SELECT id, name, birth_date, "group" FROM students WHERE name ILIKE %s
        """
        cursor = self.execute_query_return_cursor(
            select_query,
            f"search students by name {partial_name}",
            ("%" + partial_name + "%",),
        )
        if cursor:
            result = cursor.fetchall()
            cursor.close()
            return result
        return None

    def update_score(self, student_id: int, new_score: int) -> None:
        update_query = """
        UPDATE scores SET score = %s WHERE student_id = %s
        """
        cursor = self.execute_query_return_cursor(
            update_query,
            "update student's score",
            (new_score, student_id),
        )
        if cursor:
            changes_number = cursor.rowcount
            cursor.close()
            if changes_number == 0:
                print(f"No student with id {student_id} found.")
            else:
                print(
                    f"Student with id {student_id} now has {new_score} points."
                )


def main() -> None:
    connection = create_connection(
        db_name=environ.get("POSTGRES_DB"),
        db_user=environ.get("POSTGRES_USER"),
        db_password=environ.get("POSTGRES_PASSWORD"),
        db_host=environ.get("DB_HOST"),
        db_port=environ.get("DB_PORT"),
    )
    if connection is None:
        return
    connection.autocommit = True
    try:
        create_table = CreateTable(connection)
        create_table.create_students_table()
        create_table.create_scores_table()
        insert_data = InsertData(connection)
        insert_data.insert_students()
        insert_data.insert_scores()
    except (ProgrammingError, OperationalError, IntegrityError):
        connection.close()
        print("Critical error occurred!")
        return
    UpdateData(connection).update_group(5, 10)

    DeleteData(connection).delete_group(2)

    fetched_students = QueryData(connection).fetch_group(8)
    print_table(fetched_students, ["id", "name", "birth_date"])

    ComplexQuery(connection).calculate_group_average_score(8)

    additional_features = AdditionalFeatures(connection)
    sorted_scores = additional_features.fetch_sorted_data("scores", "score")
    print_table(sorted_scores, ["id", "student_id", "score"])
    fetched_students = additional_features.search_student_by_name("tracy")
    print_table(fetched_students, ["id", "name", "birth_date", "group"])
    additional_features.update_score(3, 100)
    connection.close()


if __name__ == "__main__":
    main()
