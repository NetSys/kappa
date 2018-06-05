import ast

from typing import Set, Union


class IdentifyIgnore(ast.NodeVisitor):
    """
    Walks an AST and gathers all AST nodes that should be ignored by the compiler.

    An AST node is ignored if:
      - it is a `ClassDef` or a `FunctionDef`, and
      - its docstring ends with the incantation `kappa:ignore` (whitespace ignored).

    The developer should set a class or function definition as "ignored" if:
      - the definition uses features that are not supported by the compiler; or,
      - the function/class can never pause and should avoid transformation due to performance reasons.
    """

    INCANTATION = "kappa:ignore"  # Put at end of docstrings.

    def __init__(self) -> None:
        super(IdentifyIgnore, self).__init__()
        self.ignored_nodes: Set[ast.AST] = set()

    def _visit_def(self, defn: Union[ast.ClassDef, ast.FunctionDef]) -> None:
        """Visits a class/function definition."""
        assert defn.body, "A definition cannot have an empty body"
        first_node = defn.body[0]
        if isinstance(first_node, ast.Expr) and isinstance(first_node.value, ast.Str):
            docstring = first_node.value.s
            if docstring.rstrip().endswith(self.INCANTATION):
                self.ignored_nodes.add(defn)

    def visit_FunctionDef(self, func_def: ast.FunctionDef) -> None:
        self._visit_def(func_def)

    def visit_ClassDef(self, class_def: ast.ClassDef) -> None:
        self._visit_def(class_def)


def identify_ignore(mod: ast.Module) -> Set[ast.AST]:
    """Returns a set of AST nodes from this module that should be ignored by the compiler."""
    identify = IdentifyIgnore()
    identify.visit(mod)
    return identify.ignored_nodes
