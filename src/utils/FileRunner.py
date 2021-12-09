import sys
from src.manager.TransactionManager import TransactionManager
from src.utils.FileLoader import *
from model.Site import Site


def init_sites():
    """
    Initialize sites and return list of sites

    :return: list of sites
    """
    return [Site(idx) for idx in range(1, 10 + 1)]


def run(operations):
    """
    Run the program and save the result

    :param operations: possible operations
    :return: None
    """
    transaction_manager = TransactionManager()
    transaction_manager.get_all_sites(init_sites())

    time_stamp = 0
    for operation in operations:
        # time_stamp starts from 1
        time_stamp += 1
        transaction_manager.execute_operation(operation)

    while transaction_manager.waiting_list:
        time_stamp += 1
        op = transaction_manager.waiting_list.pop(0)
        op.set_time(time_stamp)
        transaction_manager.retry()


def run_by_file(input, output):
    with open(output, "w") as f:
        sys.stdout = f
        loader = FileLoader(input)
        loader.write_to_operations()
        ops = loader.operations
        run(ops)
