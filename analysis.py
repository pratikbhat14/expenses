from database import dbConection
from pipeline import sparkasse_dataingest
import pandas as pd
import json
import numpy as np

with open("categories.json", "r") as f:
    KEY_DICT = json.load(f)

def get_dataframe(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01"):

    db = dbConection(db_file=db_file)
    data = db.find()
    df = db.as_dataframe(data)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
    df = df.loc[df["Date"]>pd.to_datetime(start_date)]
    df = df.loc[df["Date"]<pd.to_datetime(end_date)]
    return df

def get_expenses(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01"):
    df = get_dataframe(db_file="Expenses.db", start_date=start_date, end_date=end_date)
    return df.loc[df["Value"]<0, :].sort_values(by="Date", ascending=True)

def get_earnings(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01"):
    df = get_dataframe(db_file="Expenses.db", start_date=start_date, end_date=end_date)
    return df.loc[df["Value"]>=0, :].sort_values(by="Date", ascending=True)

def add_category_column(df, column_name, keys=None):

    if keys:
        search_string = "|".join(keys)
        df[column_name] = df["Description"].str.contains(search_string, case=False)
        return df
    else:
        return df

def categorize_expenses(df, key_dict):
    for key, value in key_dict.items():
        df = add_category_column(df, key, value)
    accounted = pd.Series([False]*df.shape[0])
    for key in key_dict.keys():
        accounted = accounted | df.loc[:, key]
    df["Unaccounted"] = ~accounted
    return df

def calculate_categorical_expenses(df, key_dict=KEY_DICT, freq="1M"):
    if freq=="1M":
        strf_string = "%B-%Y"
    elif freq=="1Y":
        strf_string = "%Y"
    result = {}
    for key in key_dict.keys():
        result[key] = {}
        total_value = round(df.groupby(key)["Value"].sum()[True], 2)
        if total_value != 0:
            result[key]["Total"] = total_value
        df_delta = df.loc[df[key], ["Value", "Date", key]]
        monthly = round(df_delta.groupby(pd.Grouper(key="Date", freq=freq)).sum(), 2)
        monthly.index = monthly.index.strftime(strf_string)
        result[key]["Monthly"] = {}
        for index, row in monthly.iterrows():
            if row["Value"] != 0:
                result[key]["Monthly"][index] = row["Value"]

  
    result["Unaccounted"] = {}
    total_value = round(df.groupby("Unaccounted")["Value"].sum()[True], 2)
    if total_value != 0:
        result["Unaccounted"]["Total"] = total_value

    df_delta = df.loc[df["Unaccounted"], ["Value", "Date", "Unaccounted"]]
    monthly = round(df_delta.groupby(pd.Grouper(key="Date", freq=freq)).sum(), 2)
    monthly.index = monthly.index.strftime(strf_string)
    result["Unaccounted"]["Monthly"] = {}
    for index, row in monthly.iterrows():
        if row["Value"] != 0:
            result["Unaccounted"]["Monthly"][index] = row["Value"]

    return result

def add_category_name(df, key_dict=KEY_DICT):
    category_columns = list(key_dict.keys())
    df.loc[:, "Category"] = "Unaccounted"
    cat_bool = False
    for index, row in df.iterrows():
        for cat in category_columns:
            if row[cat]==True:
                df.loc[index, "Category"] = cat
                break
    return df



def get_uncategorized_expenses(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01", key_dict=KEY_DICT):

    df = get_expenses(db_file="Expenses.db", start_date=start_date, end_date=end_date)
    df = categorize_expenses(df, key_dict)
    accounted = pd.Series([False]*df.shape[0])

    for key in key_dict.keys():
        accounted = accounted | df.loc[:, key]
        df["Unaccounted"] = ~accounted
    return df.loc[~accounted, :]

def generate_monthly_report(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01", key_dict=KEY_DICT):
    df = get_expenses(db_file, start_date=start_date, end_date=end_date)
    df = categorize_expenses(df, key_dict=key_dict)
    return calculate_categorical_expenses(df=df, key_dict=key_dict, freq="1M")

def generate_yearly_report(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01", key_dict=KEY_DICT):
    df = get_expenses(db_file, start_date=start_date, end_date=end_date)
    df = categorize_expenses(df, key_dict=key_dict)
    return calculate_categorical_expenses(df=df, key_dict=key_dict, freq="1Y")