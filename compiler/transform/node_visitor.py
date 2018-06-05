import ast

from typing import Optional


class NodeNotSupportedError(Exception):
    """Exception indicating that an AST node type is not supported by a node visitor."""

    DEFAULT_MESSAGE = "Unsupported AST node"

    def __init__(self, node: ast.AST, message: Optional[str] = None) -> None:
        """Constructs this exception; takes as argument the encountered unsupported node."""
        message = message or self.DEFAULT_MESSAGE
        super(NodeNotSupportedError, self).__init__(f"{message}: {ast.dump(node)}")


class MyNodeVisitor(object):
    """
    Same as `NodeVisitor` in the `ast` package, except that `visit()`:
      - can take extra arguments;
      - can ignore specified AST nodes (runs special handler); and
      - by default, raises `NodeNotSupportedError` if called on an AST node type without an explicit visitor.

    Given the latter difference, any subclass needs to manually handle recursing into the children of an AST node.
    """
    def __init__(self) -> None:
        self.count = 0

    def visit(self, node: ast.AST, *args, **kwargs):
        """Dispatches to handler for node type."""
        method = 'visit_' + node.__class__.__name__
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node, *args, **kwargs)

    def generic_visit(self, node: ast.AST, *args, **kwargs):
        """Called for nodes without an explicit handler."""
        raise NodeNotSupportedError(node)
