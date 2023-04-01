"""
Set up a SQL database to store expense info
"""

import os
import sqlite3
from sqlite3 import Error
import pandas as pd
from expense import Expense

class dbConection:

    def __init__(self, db_file=None):

        if db_file:
            self.conn = sqlite3.connect(db_file)
        else:
            self.conn = sqlite3.connect(":memory:")
        self.table_name = "expenses"
        self. default_table_query = f"""CREATE TABLE {self.table_name} (
                                    date text,
                                    value real,
                                    description text
                                )"""

    def create_table(self, query):
        with self.conn:
            c = self.conn.cursor()
            c.execute(query)

    def add_expense(self, exp):

        with self.conn:
            c = self.conn.cursor()
            c.execute(f"INSERT INTO {self.table_name} VALUES (:date, :value, :description)", {'date': exp.date, 'value': exp.amount, 'description': exp.text})

    def find(self, query=None):
        c = self.conn.cursor()
        if query:
            query = f"SELECT * FROM {self.table_name}" + " " + query
        else:
            query = f"SELECT * FROM {self.table_name}"
        c.execute(query)
        return c.fetchall()

    def find_text_contains(self, search_column, search_key):
        query = f"WHERE {search_column} LIKE '%{search_key}%'"
        return self.find(query)

    def as_dataframe(self, input):
        df = pd.DataFrame(input, columns=["Date", "Value", "Description"])
        return df


if __name__=="__main__":

    db = dbConection()

    db.create_table(db.default_table_query)

    exp = Expense(127.85, "Taxes", "2021-08-08")

    db.add_expense(exp)
