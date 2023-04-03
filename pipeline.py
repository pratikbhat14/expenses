import pandas as pd
from expense import Expense
from database import dbConection
from PyPDF2 import PdfReader
import re


def sparkasse_dataingest(csv_input, database=":memory:"):
    """
    Ingests data from sparkasse csv export into expenses database
    """
    df = pd.read_csv(csv_input, delimiter=";", encoding="unicode_escape")
    df["Betrag"] = df["Betrag"].str.replace(",", "."). astype(float)
    df["Buchungstag"] = pd.to_datetime(df["Buchungstag"], format="%d.%m.%y")
    df["Valutadatum"] = pd.to_datetime(df["Valutadatum"], format="%d.%m.%y")

    db = dbConection(db_file=database)
    db.create_table(db.default_table_query)

    expenses = []
    for index, row in df.iterrows():
            exp = Expense(row["Betrag"], f'{row["Buchungstext"]} : {row["Verwendungszweck"]} : {row["Beguenstigter/Zahlungspflichtiger"]}', row["Valutadatum"].strftime("%Y-%m-%d"))
            expenses.append(exp)
            db.add_expense(exp)

    return db, df

def paypal_cc_sparkasse_dataingest(csv_paypal, csv_sparkasse, text_cc, database=":memory:"):
    """
    Ingests data from sparkasse and paypal csv export into expenses database
    """
    dfs = pd.read_csv(csv_sparkasse, delimiter=";", encoding="unicode_escape")
    dfs["Betrag"] = dfs["Betrag"].str.replace(",", "."). astype(float)
    dfs["Buchungstag"] = pd.to_datetime(dfs["Buchungstag"], format="%d.%m.%y")
    dfs["Valutadatum"] = pd.to_datetime(dfs["Valutadatum"], format="%d.%m.%y")

    dfp = pd.read_csv(csv_paypal)
    dfp["Date"] = pd.to_datetime(dfp["Date"], format="%d/%m/%Y")
    dfp["Net"] = dfp["Net"].str.replace(".", "")
    dfp["Net"] = dfp["Net"].str.replace(",", ".").astype(float)
    dfp = dfp[dfp["Currency"]=="EUR"]

    with open(text_cc, "r") as f:
        rows = f.readlines()

    regex_pattern = r"(\d{1,2})\/(\d{1,2})\/(\d{2})\s(.+?)\s(-?€\d{1,3}(,\d{3})*(\.\d{2})?)"

    unmatched = []
    data = []
    for row in rows:
        match = re.match(regex_pattern, row)
        if match:
            data.append([f"{match.group(1)}/{match.group(3)}/{match.group(3)}", match.group(4), match.group(5)])
        else:
            print(f"No match found for string: {row}")
            unmatched.append(row)
    dfc = pd.DataFrame(data, columns=["Date", "Text", "Value"])
    dfc["Date"] = pd.to_datetime(dfc["Date"], format=("%m/%d/%y"))
    dfc["Value"] = dfc["Value"].str.replace("€", "")
    dfc["Value"] = dfc["Value"].str.replace(",", "")
    dfc["Value"] = dfc["Value"].astype(float)

        

    db = dbConection(db_file=database)
    db.create_table(db.default_table_query)

    return dfs, dfp, dfc
    


if __name__=="__main__":

    dfs, dfp, dfc = paypal_cc_sparkasse_dataingest(csv_paypal="paypal_transaction.csv", csv_sparkasse="20230401-1073057786-umsatz.csv", text_cc="credit_card.txt")

    print(dfs.head())
    print(dfp.head())
    print(dfc.head())

    
  
