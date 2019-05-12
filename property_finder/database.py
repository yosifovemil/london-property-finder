from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy.sql import text
import os
import pandas as pd


class Database:
    def __init__(self, database_path, reset_db=False):
        self._database_path = database_path
        self._engine = create_engine('sqlite:///%s' % self._database_path)

        if reset_db:
            print("Resetting the DB")
            os.remove(self._database_path)

        # initialise database if it does not exist yet or requested to do so
        if not os.path.exists(self._database_path):
            self._init_database()

    def read_table(self, table):
        connection = self._get_connection()
        return pd.read_sql_table(table, connection)

    def read_from_sql(self, sql):
        connection = self._get_connection()
        return pd.read_sql(sql, connection)

    def write_table(self, data, table):
        connection = self._get_connection()
        data.to_sql(table, connection, if_exists='append', index=False)

    def truncate_table(self, table):
        self.run_sql('DELETE FROM %s' % table)

    def run_sql(self, sql):
        connection = self._get_connection()
        sql_command = text(sql)
        connection.execute(sql_command)

    def _get_connection(self):
        return self._engine.connect()

    def _init_database(self):
        meta = MetaData()

        Table('Property', meta,
              Column(name='ID', type_=Integer, primary_key=True, nullable=False),
              Column(name='Price', type_=Integer, nullable=True),
              Column(name='Type', type_=String, nullable=True),
              Column(name='URL', type_=String, nullable=False),
              Column(name='Bedrooms', type_=Integer, nullable=True))

        Table('Location', meta,
              Column(name='ID', type_=Integer, primary_key=True, nullable=False),
              Column(name='Latitude', type_=Float, nullable=True),
              Column(name='Longitude', type_=Float, nullable=True))

        Table('LSOA', meta,
              Column(name='ID', type_=Integer, primary_key=True, nullable=False),
              Column(name='LSOA', type_=String, nullable=True),
              Column(name='MultipleDeprivationIndex', type_=String, nullable=True))

        Table('Travel', meta,
              Column(name='ID', type_=Integer, primary_key=True, nullable=False),
              Column(name='JourneyDuration', type_=Float, nullable=False),
              Column(name='JourneyFare', type_=Float, nullable=False),
              Column(name='Walking', type_=Float, nullable=False),
              Column(name='Train', type_=Float, nullable=False),
              Column(name='Underground', type_=Float, nullable=False),
              Column(name='Bus', type_=Float, nullable=False))

        meta.create_all(self._engine)
