import ast
from typing import Set, Union, List

from .util import parse_ast_stmt


class InsertAutoPause(ast.NodeTransformer):
    """
    Inserts opportunistic checkpoint calls into a flattened AST.  Mutates the AST.

    The resulting AST still looks flattened.

    Opportunistic checkpoint calls are inserted before every function invocation.
    """
    # TODO(zhangwen): maybe insert checkpoint calls in other places, e.g., at the end of a loop?

    to_insert: List[ast.AST] = [
        # TODO(zhangwen): these "flattened" statements look ugly.
        parse_ast_stmt("_ = rt.maybe_pause"),
        parse_ast_stmt("_ = _()"),
    ]

    def __init__(self, ignored: Set[ast.AST]) -> None:
        super(InsertAutoPause, self).__init__()
        self._ignored = ignored

    def visit_Assign(self, ass: ast.Assign) -> Union[ast.AST, List[ast.AST]]:
        # Every function call is turned into an Assign by flatten.
        rhs = ass.value
        if not isinstance(rhs, ast.Call):
            return ass

        return self.to_insert + [ass]

    def generic_visit(self, node):
        if node in self._ignored:
            return node

        return super(InsertAutoPause, self).generic_visit(node)


def insert_auto_pause(mod: ast.Module, ignored: Set[ast.AST]) -> ast.Module:
    return InsertAutoPause(ignored=ignored).visit(mod)
