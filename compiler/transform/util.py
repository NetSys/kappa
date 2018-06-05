import ast
import textwrap
from collections import defaultdict

from typing import Set, DefaultDict, Type, TypeVar

VarsByUsageType = DefaultDict[Type[ast.expr_context], Set[str]]


class _FindVariablesByUsageVisitor(ast.NodeVisitor):
    """Traverses the AST, finds variables, and group their names by usage.

    Don't instantiate this class directly.  Instead, use the `find_variables_by_usage` function defined below.
    """
    def __init__(self) -> None:
        # The keys are subtypes of `ast.expr_context` -- `Load`, `Store`, etc.
        self.vars_by_usage: VarsByUsageType = defaultdict(set)
        super(_FindVariablesByUsageVisitor, self).__init__()

    def visit_Name(self, name: ast.Name) -> None:
        self.vars_by_usage[type(name.ctx)].add(name.id)


def find_variables_by_usage(node: ast.AST) -> VarsByUsageType:
    """Returns a list of variable names grouped by usage context."""
    visitor = _FindVariablesByUsageVisitor()
    visitor.visit(node)
    return visitor.vars_by_usage


def load(symbol_id: str) -> ast.Name:
    """Returns an AST Name node that loads a variable."""
    return ast.Name(id=symbol_id, ctx=ast.Load())


def assign(symbol_id: str, value: ast.expr) -> ast.Assign:
    """Returns an AST Assign node that assign a value to a variable."""
    return ast.Assign(targets=[ast.Name(id=symbol_id, ctx=ast.Store())], value=value)


AST_T = TypeVar("AST_T", bound=ast.AST)


def clone_node(node: AST_T, **updated_args) -> AST_T:
    """Returns a shallow copy of an AST node with the specified attributes updated."""
    args = dict(ast.iter_fields(node))
    args.update(updated_args)
    ast_class = type(node)
    return ast_class(**args)


def parse_ast_expr(expr_code: str) -> ast.expr:
    """Parses code for an expression into an AST."""
    expr_code = textwrap.dedent(expr_code)
    node = ast.parse(expr_code).body[0]
    assert isinstance(node, ast.Expr)
    return node.value


def parse_ast_stmt(stmt_code: str) -> ast.stmt:
    """Parses code for a single statement into an AST."""
    stmt_code = textwrap.dedent(stmt_code)
    node = ast.parse(stmt_code, mode="exec")
    assert isinstance(node, ast.Module)
    if len(node.body) > 1:
        raise ValueError(f"Code contains more than one statement: {stmt_code}")
    return node.body[0]
