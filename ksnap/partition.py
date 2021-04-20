"""Read/write partition
"""
from ksnap.message import Message
import sqlite3

from dataclasses import dataclass
from typing import List


class PartitionFileReadError(Exception):
    pass


@dataclass
class Partition:

    topic: str
    name: int
    messages: List[Message]

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
            "key BLOB NOT NULL, "
            "message BLOB, "
            "timestamp INTEGER, "
            "headers TEXT)"
        )
        cursor.execute(create_table_query)
        conn.commit()
        cursor.executemany("INSERT INTO data VALUES(?,?,?,?,?);",
                           [m.to_row() for m in self.messages])
        conn.commit()
        conn.close()

    @classmethod
    def from_file(cls, db_file_path: str):
        try:
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
        except Exception as exc:
            raise PartitionFileReadError from exc
        finally:
            conn.close()
        return cls(topic, name, [Message.from_row(*r) for r in rows])
