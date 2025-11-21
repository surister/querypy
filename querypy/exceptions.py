class QueryEngineError(BaseException):
    """Errors raised by the QueryEngine"""

    pass


class UnknownColumnError(QueryEngineError):
    def __init__(self, col: str):
        self.message = f"No column named {col!r}"
