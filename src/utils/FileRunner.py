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
        transaction_manager.execute_operation(operation)
        time_stamp += 1

    while transaction_manager.waiting_list:
        time_stamp += 1
        op = transaction_manager.waiting_list.pop(0)
        op.set_time(time_stamp)
        is_succeed = transaction_manager.retry()
        if not is_succeed:
            # op = transaction_manager.waiting_list.pop(0)
            print(f'The operation {op.get_type()} {op.get_tid()} failed')


def run_by_file(input, output):
    with open(output, "w") as f:
        sys.stdout = f
        loader = FileLoader(input)
        loader.write_to_operations()
        ops = loader.operations
        run(ops)


def run_by_step():
    """
    Run program step by step

    :return: None
    """
    trans_manager = TransactionManager()
    tick = 0

    while True:
        command = input("RepCRec >: ")
        try:
            if command == "refresh":
                trans_manager = TransactionManager()
                tick = 0
            elif command == "<END>":
                while trans_manager.waiting_list:
                    cur_blocked_size = len(trans_manager.waiting_list)
                    # tick += 1
                    trans_manager.retry()

                    if cur_blocked_size == len(trans_manager.blocked):
                        print("Following operation can not be executed, maybe the test case is not terminable:")
                        for op in trans_manager.blocked:
                            print(op)
                        break

                trans_manager = TransactionManager()
                tick = 0

            elif command == "quit":
                print("bye")
                break
            else:
                tick += 1

                operation = FileLoader.parse_line(command, 1)
                print(operation.get_type())
                print(operation.get_tid())
                print(operation.get_time())
                try:
                    trans_manager.execute_operation(operation)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

