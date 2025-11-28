class QueryEngineError(BaseException):
    """Errors raised by the QueryEngine"""
    pass

class LogicalError(QueryEngineError):
    pass

class UnknownColumnError(QueryEngineError):
    def __init__(self, col: str):
        super().__init__(f"No column named {col!r}")
