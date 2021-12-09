import prettytable
from src.utils.FileLoader import FileLoader
from src.model.Operation import Operation

def print_result(headers, rows):
    """
    Print the query result using pretty table, only for dump operation now

    :param headers: table headers
    :param rows: table rows
    :return: None
    """
    table = PrettyTable()
    table.field_names = headers
    for row in rows:
        table.add_row(row)
    print(table)


def print_commit_result(tid):
    print(f"Transaction T{tid} commit")
