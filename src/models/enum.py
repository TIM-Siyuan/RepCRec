from enum import Enum


class site_status(Enum):
    UP = 1
    DOWN = 2


class transaction_type(Enum):
    RO = 1
    RW = 2


class transaction_status(Enum):
    ACTIVE = 1
    BLOCKED = 2
    COMMITED = 3
    ABORTED = 4


class operation_type(Enum):
    BEGIN = 1
    BEGINRO = 2
    WRITE = 3
    READ = 4
    FAIL = 5
    RECOVER = 6
    DUMP = 7
    END = 8


class lock_type(Enum):
    READ = 1
    WRITE = 2


class data_type(Enum):
    REPLICATED = 1
    NONREPLICATED = 2
