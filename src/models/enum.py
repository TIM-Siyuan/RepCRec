from enum import Enum


class SiteStatus(Enum):
    UP = 1
    DOWN = 2


class TransactionType(Enum):
    RO = 1
    RW = 2


class TransactionStatus(Enum):
    ACTIVE = 1
    BLOCKED = 2
    COMMITED = 3
    ABORTED = 4


class OperationType(Enum):
    BEGIN = 1
    BEGINRO = 2
    WRITE = 3
    READ = 4
    FAIL = 5
    RECOVER = 6
    DUMP = 7
    END = 8


class LockType(Enum):
    READ = 1
    WRITE = 2
