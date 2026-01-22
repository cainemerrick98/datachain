from ..models import SQLQuery

class BaseSQLCompiler():
    """
    Abstract implementation of what a SQLCompiler needs to implement
    """
    def compile(query: SQLQuery) -> str:
        raise NotImplementedError()