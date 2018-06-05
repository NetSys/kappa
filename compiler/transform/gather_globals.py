"""
Finds names that are global to a module, e.g., builtins, functions, and classes.

Any references to these identifiers need not be captured by a continuation because they're available globally within
the module.  This optimization not only reduces the capture, but also deals with objects that pickle cannot handle in
the first place, e.g., module names.

When omitting captured variables, keep in mind that global names can be shadowed inside functions.
"""
# TODO(zhangwen): this optimization assumes that global variables cannot be mutated by functions.
import ast
import builtins
from typing import Set

from .util import find_variables_by_usage


def gather_global_names(mod: ast.Module) -> Set[str]:
    """Returns names of globals, whose values presumably don't change."""
    names = set(dir(builtins))  # Start with the builtins.

    for stmt in mod.body:
        if isinstance(stmt, (ast.ClassDef, ast.FunctionDef)):
            names.add(stmt.name)
        elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
            for alias in stmt.names:
                names.add(alias.asname or alias.name)
        else:
            names.update(find_variables_by_usage(stmt)[ast.Store])

    return names
