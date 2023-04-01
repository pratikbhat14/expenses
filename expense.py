"""
Expense Class
"""
import datetime
class Expense:
    """
    Expense class encapsulates information on individual expenses
    """
    def __init__(self, amount, text, date) -> None:
        self.amount = amount
        self.text = text
        self.date = date


    def __repr__(self):
        return f'Expense: {self.amount}, on {self.date}, Description: {self.text}'


if __name__=="__main__":

    exp = Expense(200.13, "Leisure Stroe", "2022-08-08")

