import prettytable
from src.utils.FileLoader import FileLoader
from src.model.Operation import Operation

# f = FileLoader("D:/Courses/NYU MSCS/2021 Fall/RepCRec/src/Tests/Test3.txt")
# f.write_to_operations()
# for op in f.operations:
#     print(op.type, op.Tid, op.vid, op.value, op.sid, op.time)


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
    print(f"Transaction {tid} commit")
