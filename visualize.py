"""
Plot Graphs
"""

import matplotlib.pyplot as plt
import seaborn as sns



def plot_pie(df, x_col, y_col, label_col):
    plt.figure(figsize=(6, 6))
    plt.title("Pie Chart")
    sns.pie(data=df, x=x_col, y=y_col, labels=label_col, autopct="%1.1f")
    plt.show()




if __name__=="__main__":

    sns.set_style("darkgrid")

    from database import dbConection
    from pipeline import sparkasse_dataingest
    import pandas as pd
    import json
    import numpy as np

    from analysis import generate_monthly_report, generate_yearly_report, categorize_expenses, get_expenses, add_category_name


    db_file = "Expenses.db"
    with open("categories.json", "r") as f:
        KEY_DICT=json.load(f)

    df = get_expenses(db_file="Expenses.db", start_date="2020-05-31", end_date="2023-04-01")
    df = categorize_expenses(df, KEY_DICT)
    df = add_category_name(df, key_dict=KEY_DICT)
    category_map = {"Cash_Expense": "Regular Expense",
                    "Credit_Card": "One Time Expense",
                    "Eating_Out": "Leisure",
                    "Entertainment": "Leisure",
                    "Fixed_Expenses": "Regular Expense",
                    "Investment": "Investment",
                    "Medical": "One Time Expense",
                    "One_Time_Expenses": "One Time Expense",
                    "Supermarket": "Regular Expense",
                    "Transport": "One Time Expense",
                    "Unaccounted": "Unaccounted"}
    df["Category"] = df["Category"].replace(category_map)
    category_expenses = df.groupby(['Category', pd.Grouper(key='Date', freq='MS')])['Value'].sum().abs().reset_index()
    category_expenses = category_expenses.pivot(index='Date', columns='Category', values='Value')
    category_expenses.drop(columns=["Investment"], inplace=True)
    category_expenses.fillna(0, inplace=True)
    category_expenses = category_expenses.reset_index()
    print(category_expenses)
    date_labels = category_expenses['Date'].dt.strftime('%b-%y')

    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax = sns.barplot(data=category_expenses, x="Date", y='Regular Expense', color=sns.xkcd_rgb["ocean blue"], label="Regular Expense")
    ax = sns.barplot(data=category_expenses, x="Date", y='Leisure', bottom=category_expenses["Regular Expense"], color=sns.xkcd_rgb["light olive"], label="Leisure")
    ax = sns.barplot(data=category_expenses, x="Date", y='One Time Expense', bottom=category_expenses["Leisure"]+category_expenses["Regular Expense"], color=sns.xkcd_rgb["terracotta"], label="One Time Expense")
    ax = sns.barplot(data=category_expenses, x="Date", y='Unaccounted', bottom=category_expenses["One Time Expense"]+category_expenses["Leisure"]+category_expenses["Regular Expense"], color=sns.xkcd_rgb["charcoal"], label="Unaccounted")
    ax.set_xticklabels(date_labels)
    ax.set_ylabel("Expenses [EUR]")
    ax.set_xlabel("Month-Year")
    ax.tick_params(axis='x', rotation=90)
    ax.legend()
    ax.set_title("Distribution of Monthly Expenses")
    plt.savefig("monthly_expenses.png")
    plt.show()
    



    
    
