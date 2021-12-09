import PrettyTable
from src.utils.FileLoader import FileLoader
from src.manager.TransactionManager import TransactionManager
from src.model.Operation import Operation

f = FileLoader("D:/Courses/NYU MSCS/2021 Fall/RepCRec/src/Tests/Test3.txt")
f.write_to_operations()
for op in f.operations:
    print(op.type, op.Tid, op.vid, op.value, op.sid, op.time)


def run(operations):
    """
    Run the program and save the result

    :param operations: possible operations
    :return: None
    """
    transaction_manager = TransactionManager()

    time_stamp = 0
    for operation in operations:
        transaction_manager.execute_operation(operation)
        time_stamp += 1

    while transaction_manager.waiting_list:
        time_stamp += 1
        is_succeed = transaction_manager.retry()
        if not is_succeed:
            op = transaction_manager.waiting_list.pop(0)
            print(f'The operation {op} failed', op)


def run_by_file(input, output):
    with open(output, "w") as f:
        sys.stdout = f
        loader = FileLoader(input)
        loader.write_to_operations()
        ops = loader.operations
        run(ops)


def run_by_step():
    pass


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
