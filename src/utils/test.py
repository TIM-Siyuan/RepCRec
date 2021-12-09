from src.model.Operation import operation
from src.model.enum import operation_type
from src.utils.IOUtils import FileLoader


f = FileLoader("D:/Courses/NYU MSCS/2021 Fall/RepCRec/src/Tests/Test3.txt")
f.write_to_operations()
for op in f.operations:
    print(op.type, op.Tid, op.vid, op.value, op.sid, op.time)