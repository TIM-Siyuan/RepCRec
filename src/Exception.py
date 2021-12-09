class InvalidOperationError(Exception):
    """Error thrown when the operation is invalid."""

    def __init__(self, message):
        self.message = message
