"""
Main.py
"""

from database import dbConection
from pipeline import sparkasse_dataingest
import pandas as pd
import json
import numpy as np

from analysis import generate_monthly_report, generate_yearly_report, get_uncategorized_expenses


if __name__=="__main__":

    db_file = "Expenses.db"
    with open("categories.json", "r") as f:
        KEY_DICT=json.load(f)

    #CREATE DATABASE
    # db, df_raw = sparkasse_dataingest(csv_input="20230401-1073057786-umsatz.csv", database=db_file)

    # GENERATE MONTHLY REPORT
    with open("monthly_expenses.json", "w") as f:
        json.dump(generate_monthly_report(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01", key_dict=KEY_DICT), f, indent=4)

    #GENERATE YEARLY REPORT
    with open("yearly_expenses.json", "w") as f:
        json.dump(generate_yearly_report(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01", key_dict=KEY_DICT), f, indent=4)


    #UNCATEGORIZED EXPENSES
    df = get_uncategorized_expenses(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01", key_dict=KEY_DICT)
    df.to_csv("uncatgorized.csv")


    




