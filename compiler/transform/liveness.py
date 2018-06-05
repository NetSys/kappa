import ast

from typing import List, Union, Set

from .node_visitor import MyNodeVisitor
from .util import find_variables_by_usage


class LivenessTracker(MyNodeVisitor):
    """
    Keeps track of live variables during the CPS transformation.

    When the transformation walks the program backwards, a `LivenessVisitor` instance remembers a set of which
    variables are live at the current program point.  When a new statement is _prepended_ to the list of considered
    statements (remember we're walking the program backwards), the set of live variables is updated.

    Initially, no variable is live.  A function's return value is made live when prepending the `return` statement.
    """
    # FIXME(zhangwen): make sure liveness tracking works even with statements / expressions that are not flattened.

    def __init__(self) -> None:
        super(LivenessTracker, self).__init__()
        self._live_vars: Set[str] = set()

    @property
    def live_vars(self) -> Set[str]:
        """Returns the names of currently live variables, as as set."""
        return self._live_vars.copy()  # Return a copy so that this instance's copy isn't messed with.

    def clone(self) -> "LivenessTracker":
        """Returns a copy of this instance."""
        my_copy = LivenessTracker()
        my_copy._live_vars = self._live_vars.copy()
        return my_copy

    def prepend_stmt(self, stmt: ast.stmt) -> None:
        """Prepends a statement to the list of considered statements and updates the set of live variables."""
        return self.visit(stmt)

    def visit_simple_stmt(self, stmt: ast.stmt) -> None:
        # TODO(zhangwen): make sure the statement is simple?
        vars_by_usage = find_variables_by_usage(stmt)
        # A variable could be both used and written to.
        self._live_vars -= vars_by_usage[ast.Store]
        self._live_vars |= vars_by_usage[ast.Load]

    def visit_stmt_list(self, stmts: List[ast.stmt]) -> None:
        """Simply visits the statements in reverse order."""
        for stmt in reversed(stmts):
            self.prepend_stmt(stmt)

    # Each `visit` method updates the live variable set after prepending the statement passed in.
    def visit_Expr(self, expr: ast.Expr) -> None:
        self.visit_simple_stmt(expr)

    def visit_Assert(self, asr: ast.Assert) -> None:
        self.visit_simple_stmt(asr)

    def visit_Assign(self, assign: ast.Assign) -> None:
        self.visit_simple_stmt(assign)

    def visit_AnnAssign(self, ann_assign: ast.AnnAssign) -> None:  # type: ignore
        self.visit_simple_stmt(ann_assign)

    def visit_AugAssign(self, aug_assign: ast.AugAssign) -> None:
        self.visit_simple_stmt(aug_assign)
        # The variable assigned to is also live (`x` in `x += 5`).
        self._live_vars |= find_variables_by_usage(aug_assign.target)[ast.Store]

    def visit_Break(self, _br: ast.Break) -> None:
        pass

    def visit_ClassDef(self, _class_def: ast.ClassDef) -> None:
        # TODO(zhangwen): finish this?
        pass

    def visit_Continue(self, _cont_stmt: ast.Continue) -> None:
        pass

    def visit_FunctionDef(self, func_def: ast.FunctionDef) -> None:
        self.visit_stmt_list(func_def.body)
        # The arguments aren't live before the function definition.
        self._live_vars -= find_variables_by_usage(func_def.args)[ast.Param]
        for decorator in func_def.decorator_list:
            self._live_vars |= find_variables_by_usage(decorator)[ast.Load]

    def visit_If(self, if_stmt: ast.If) -> None:
        body_tracker = self.clone()
        body_tracker.visit_stmt_list(if_stmt.body)
        body_live_vars = body_tracker.live_vars

        orelse_tracker = self.clone()
        orelse_tracker.visit_stmt_list(if_stmt.orelse)
        orelse_live_vars = orelse_tracker.live_vars

        self._live_vars = body_live_vars | orelse_live_vars | find_variables_by_usage(if_stmt.test)[ast.Load]

    def _visit_import(self, imp: Union[ast.Import, ast.ImportFrom]) -> None:
        """Common between `Import` and `ImportFrom`."""
        imported_vars = set()
        for alias in imp.names:
            name = alias.asname or alias.name
            imported_vars.add(name)
        # The imported variables are essentially "assigned to" by the import.
        self._live_vars -= imported_vars

    def visit_Import(self, imp: ast.Import) -> None:
        self._visit_import(imp)

    def visit_ImportFrom(self, imp_from: ast.ImportFrom) -> None:
        self._visit_import(imp_from)

    def visit_Pass(self, _pass_stmt: ast.Pass) -> None:
        pass

    def visit_While(self, while_stmt: ast.While) -> None:
        assert not while_stmt.orelse
        self.visit_stmt_list(while_stmt.body)
        self._live_vars |= find_variables_by_usage(while_stmt.test)[ast.Load]

    def visit_For(self, for_stmt: ast.For) -> None:
        assert not for_stmt.orelse
        self.visit_stmt_list(for_stmt.body)
        self._live_vars -= find_variables_by_usage(for_stmt.target)[ast.Store]
        self._live_vars |= find_variables_by_usage(for_stmt.iter)[ast.Load]

    def visit_Return(self, ret: ast.Return) -> None:
        self.visit_simple_stmt(ret)
