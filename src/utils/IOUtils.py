from src.manager.TransactionManager import TransactionManager
from src.model.Operation import Operation
import PrettyTable

def run(operations):
    """
    Run the program and save the result

    :param operations: possible operations
    :return: None
    """
    transaction_manager = TransactionManager()

    time_stamp = 0
    for operation in operations:
        time_stamp += 1
        op = Operation.parse_operation(operation)
        transaction_manager.two_step_execute(op, time_stamp)

    while transaction_manager.waiting_list:
        time_stamp += 1
        is_succeed = transaction_manager.retry(time_stamp)
        if not is_succeed:
            print("This operation execution failed: " + transaction_manager.waiting_list.pop(0))


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

