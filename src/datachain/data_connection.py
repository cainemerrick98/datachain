from typing import Protocol, Any
import ibis.expr.types as ir

# The below is just duck typing to create a consistent interface around Ibis connections

class IbisConnection(Protocol):
    """Protocol for an Ibis connection that can compile and execute queries."""
    name: str

    def compile(self, query: ir.Expr) -> str:...

    def table(self, name: str) -> ir.Table:...

    def execute(self, query: ir.Expr) -> Any:...


class DataConnection(IbisConnection):
    """Wrapper around an Ibis connection to provide a consistent interface."""
    def __init__(self, conn: IbisConnection):
        self.conn = conn

    @property
    def name(self) -> str:
        return self.conn.name

    def compile(self, query: ir.Expr) -> str:
        return self.conn.compile(query)

    def table(self, name: str) -> ir.Table:
        return self.conn.table(name)

    def execute(self, query: ir.Expr) -> Any:
        return self.conn.execute(query)