from src.models.Operation import operation
from src.models.enum import operation_type


class FileLoader(object):
    def __init__(self, file_name):
        self.lines = []
        self.operations = []
        with open(file_name, 'r') as f:
            for line in f.readlines():
                if line.startswith("==="):
                    break
                if not line.startswith("//"):
                    line.replace(' ', '')
                    self.lines.append(line.strip())

    # operation_type, Tid, vid, value, sid, time
    def write_to_operations(self):
        time = 0
        for line in self.lines:
            if line.find("beginRO") != -1:
                line = (line.split('('))[1]
                line = line.replace(')', '')
                line = line[1:]
                op = operation(operation_type.BEGINRO, int(line), None, None, None, time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("begin") != -1:
                line = line.split("(")[1]
                line = line.replace(')', '')
                line = line[1:]
                op = operation(operation_type.BEGIN, int(line), None, None, None, time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("W") != -1:
                line = line.split("(")[1]
                line = line.replace(')', '')
                values = line.split(",")
                values[0] = (values[0])[1:]
                values[1] = (values[1]).split("x")[1]
                op = operation(operation_type.WRITE, int(values[0]), int(values[1]), int(values[2]), None, time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("R") != -1:
                line = line.split("(")[1]
                line = line.replace(')', '')
                values = line.split(",")
                values[0] = (values[0])[1:]
                values[1] = (values[1]).split("x")[1]
                op = operation(operation_type.READ, int(values[0]), int(values[1]), None, None, time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("fail") != -1:
                line = line.split("(")[1]
                line = line.replace(')', '')
                op = operation(operation_type.FAIL, None, None, None, int(line), time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("end") != -1:
                line = line.split("(")[1]
                line = line.replace(')', '')
                line = line[1:]
                op = operation(operation_type.END, int(line), None, None, None, time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("recover") != -1:
                line = line.split("(")[1]
                line = line.replace(')', '')
                op = operation(operation_type.RECOVER, None, None, None, int(line), time)
                self.operations.append(op)
                time += 1
                continue
            if line.find("dump") != -1:
                op = operation(operation_type.DUMP, None, None, None, None, time)
                self.operations.append(op)
                time += 1
                continue
