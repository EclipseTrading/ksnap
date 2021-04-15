# Read/write partition
import sqlite3

from typing import Any, List


class Partition:
    def __init__(self, topic: str, name: int, messages: List[Any]):
        self.topic = topic
        self.name = name
        self.messages = messages

    def to_file(self, db_file_path: str):
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        create_metadata_query = (
            "CREATE TABLE IF NOT EXISTS metadata "
            "(topic TEXT, partition INTEGER)"
        )
        cursor.execute(create_metadata_query)
        conn.commit()
        cursor.execute(
            "INSERT INTO metadata VALUES(?,?);", (self.topic, self.name)
        )
        conn.commit()
        create_table_query = (
            "CREATE TABLE IF NOT EXISTS data "
            "(offset INTEGER PRIMARY KEY, "
            "key BLOB NOT NULL, message BLOB)"
        )
        cursor.execute(create_table_query)
        conn.commit()
        cursor.executemany("INSERT INTO data VALUES(?,?,?);", self.messages)
        conn.commit()
        conn.close()

    @classmethod
    def from_file(cls, db_file_path: str):
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        # read metadata
        metadata_query = "SELECT * FROM metadata LIMIT 1"
        cursor.execute(metadata_query)
        topic, name = cursor.fetchone()
        # read messages
        message_query = "SELECT * FROM data"
        cursor.execute(message_query)
        rows = cursor.fetchall()
        conn.close()
        return cls(topic, name, rows)
