class DataCopy:
    def __init__(self, data_type, initial_value):
        self.data_type = data_type
        self.read_available = True
        self.commit_history = {-1: initial_value}

    def is_read_available(self):
        return self.read_available

    def set_read_available(self, availability):
        self.read_available = availability

    def get_data_type(self):
        return self.data_type

    def add_commit_history(self, time, value):
        self.commit_history[time] = value

    def get_latest_commit(self):
        v = max(self.commit_history, key=self.commit_history.get)
        return v

    def get_commit_history(self):
        return self.commit_history

    def clear_commit_history(self):
        self.commit_history = {}
