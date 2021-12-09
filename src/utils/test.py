from src.models.operation import operation
from src.CustomizedConf import OperationType
from src.utils.FileLoader import FileLoader


f = FileLoader("D:/Courses/NYU MSCS/2021 Fall/RepCRec/Tests/Test3.txt")
f.write_to_operations()
for op in f.operations:
    print(op.type, op.tid, op.vid, op.value, op.sid, op.time)