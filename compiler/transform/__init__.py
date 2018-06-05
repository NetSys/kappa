import ast

from .auto_pause import insert_auto_pause
from .identify_ignore import identify_ignore
from .cps import transform_to_cps
from .flatten import flatten_module
from .util import parse_ast_stmt


def transform(mod: ast.Module, *, auto_pause: bool = False) -> ast.Module:
    """Transforms a module."""
    ignored_nodes = identify_ignore(mod)

    # Import runtime library.  This code should be inserted before the CPS transformation so that `rt` module imported
    # here be identified as a global and be excluded from checkpoints.
    mod.body[0:0] = [
        parse_ast_stmt("import rt"),
    ]

    mod = flatten_module(mod, ignored=ignored_nodes)

    if auto_pause:
        mod = insert_auto_pause(mod, ignored=ignored_nodes)

    mod = transform_to_cps(mod, ignored=ignored_nodes)

    # Wrap lambda handler.
    mod.body.append(parse_ast_stmt("""
    try:
        rt_handler = rt.lambda_handler(handler)
    except NameError:
        pass
    """))

    fixed_mod = ast.fix_missing_locations(mod)
    assert isinstance(fixed_mod, ast.Module)
    return fixed_mod
