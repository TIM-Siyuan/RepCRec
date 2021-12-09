import re
from src.CustomizedConf import OperatorType


class Operation:

    @staticmethod
    def parse_operation(line):
        """
        Parse operations

        :param line: read from stdIn or file
        :return: dict-- a pair of {key: val}
        """
        reg = r'(.*)\((.*?)\)'
        ops = re.search(reg, line)
        operator, operand = ops.group(1), [op.strip() for op in ops.group(2).split(",")]
        return OperatorType[operator](operand)
