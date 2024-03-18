""" db class for database """

import os
import pickle
from shutil import rmtree
from lstore.table import Table
from lstore.disk import DISK


class Database:
    """Database class for database"""

    def __init__(self) -> None:
        self.db_dir_path: str = None
        self.tables: dict[str, Table] = {}

    def open(self, db_dir_path: str) -> None:
        """
        Takes in a path from the root of the directory and opens the database at that location.

        If a database is already initialized, raises a ValueError.
        """

        if self.db_dir_path is not None:
            raise ValueError

        self.db_dir_path = db_dir_path
        DISK.set_database(db_dir_path)
        self.tables = dict()

        # load data from database if it had been created before
        if os.path.exists(db_dir_path):
            table_dirs = [
                os.path.join(db_dir_path, _)
                for _ in os.listdir(db_dir_path)
                if os.path.isdir(os.path.join(db_dir_path, _))
            ]
            for table_dir in table_dirs:
                metadata_path = os.path.join(table_dir, "metadata.pkl")
                with open(metadata_path, 'rb') as file:  # 'rb' mode to read as bytes
                    metadata = pickle.loads(file.read())
                self.tables[metadata["table_name"]] = Table(
                    metadata["table_dir_path"],
                    metadata["num_columns"],
                    metadata["key_index"],
                    metadata["num_records"],
                )

    def close(self) -> None:
        """
        Saves all tables in database to disk.

        If no database found, raise a ValueError.
        """
        if self.db_dir_path is None:
            raise ValueError("No database is currently open.")

        for table_name, table in self.tables.items():
            # Assuming each table object has a method to return its metadata as a dictionary
            metadata = {
                "table_name": table_name,
                "table_dir_path": table.table_dir_path,
                "num_columns": table.num_columns,
                "key_index": table.key_index,
                "num_records": table.num_records,
            }
            # Assuming DISK.write_metadata_to_disk writes the metadata dictionary to a file named 'metadata.pkl' in the table's directory
            DISK.write_metadata_to_disk(table.table_dir_path, metadata)

        # Optionally, clear the tables dictionary if you're closing the database
        self.tables.clear()
        self.db_dir_path = None

    def create_table(self, table_name: str, num_columns: int, key_index: int) -> Table:
        """
        Creates a new table to be inserted into the database.
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """

        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists.")

        table_dir_path = os.path.join(self.db_dir_path, table_name)
        os.makedirs(table_dir_path, exist_ok=False)

        # save metadata of table
        metadata = {
            "table_name": table_name,
            "table_dir_path": table_dir_path,
            "num_columns": num_columns,
            "key_index": key_index,
            "num_records": 0,
        }
        DISK.write_metadata_to_disk(table_dir_path, metadata)

        # create table
        self.tables[table_name] = Table(table_dir_path, num_columns, key_index, 0)
        # print(f"Table {table_name} created.")
        return self.tables[table_name]
    
    def drop_table(self, table_name: str) -> None:
        """
        Delete the specified table.

        WARNING: This will delete all data associated with the table.
        """

        if not table_name in self.tables[table_name]:
            raise ValueError
        rmtree(os.path.join(self.db_dir_path, table_name))
        del self.tables[table_name]
        # print(f"Table {table_name} dropped.")

    def get_table(self, table_name: str) -> Table:
        """
        Returns table with the passed name

        If a table isn't found, it raises a ValueError.
        """

        if table_name in self.tables:
            return self.tables[table_name]
        else:
            raise ValueError(f"Table '{table_name}' not found.")