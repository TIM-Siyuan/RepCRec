class FileLoader(object):
    def __init__(self, file_name):
        self.lines = []

        with open(file_name, 'r') as f:
            for line in f.readlines():
                if line.startswith("==="):
                    break
                if not line.startswith("//"):
                    self.lines.append(line.strip())



