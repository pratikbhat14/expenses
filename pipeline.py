import pandas as pd
from expense import Expense
from database import dbConection



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
    


if __name__=="__main__":
    db, df_raw = sparkasse_dataingest(csv_input="20230401-1073057786-umsatz.csv")

    data = db.find()

    df = db.as_dataframe(data)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
    print(df.loc[0, "Description"])
   