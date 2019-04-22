import sqlite3
import os
import sqlalchemy


CREATE_PROPERTY_TABLE = """
CREATE TABLE Property(
    ID INT NOT NULL,
    Price INT NOT NULL,
    Type VARCHAR NULL,
    URL VARCHAR NOT NULL,
    Bedrooms INT NOT NULL,
    Latitude DECIMAL NULL,
    Longitude DECIMAL NULL,
    LSOA VARCHAR NULL,
    MultipleDeprivationIndex INT NULL
);
"""

SELECT_RECORDS = "SELECT * FROM Property"


class Database:
    def __init__(self, database_path):
        self._database_path = database_path

        # initial setup, applied only if necessary
        self._setup_database()

    def _run_sql(self, sql):
        with sqlite3.connect(self._database_path) as db:
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()

    def _setup_database(self):
        if os.path.exists(self._database_path):
            # database already created, nothing to do
            return

        self._run_sql(self._database_path, CREATE_PROPERTY_TABLE)

    def read_records(self):
        #STUB
        pass
