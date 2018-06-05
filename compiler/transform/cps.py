import ast
from collections import defaultdict
from typing import List, DefaultDict, Optional, Set, Union, Tuple, NamedTuple, TYPE_CHECKING

from .gather_globals import gather_global_names
from .util import clone_node, parse_ast_expr, parse_ast_stmt, find_variables_by_usage
from .liveness import LivenessTracker
from .node_visitor import MyNodeVisitor, NodeNotSupportedError

if TYPE_CHECKING:
    from typing_extensions import NoReturn

LoopT = Union[ast.For, ast.While]


class LoopBodyDelimiter(NamedTuple):
    """Inserted into a subsequent statement list denoting that everything before it is in a loop's body."""
    loop: LoopT


SubsequentStatementsT = List[Union[ast.stmt, LoopBodyDelimiter]]


class CPSTransformerContext(object):
    def __init__(self,
                 subsequent_stmts: SubsequentStatementsT,
                 subsequent_live_vars: LivenessTracker,
                 curr_class: Optional[ast.ClassDef],
                 curr_func: Optional[ast.FunctionDef],
                 global_names: Set[str]
                ) -> None:
        # Statements executed after the current statement; to be recorded if the current statement pauses.
        self.subsequent_stmts = subsequent_stmts

        # Names of live variables; to be captured in a continuation.
        self.subsequent_liveness = subsequent_live_vars

        # The current class/function we're in; None if we're not inside any function definition.
        self.curr_class = curr_class
        self.curr_func = curr_func

        # Names of globals accessible in the current scope.
        self.global_names = global_names

    @staticmethod
    def new_context(mod: ast.Module) -> "CPSTransformerContext":
        """Generates a new context for a module."""
        global_names = gather_global_names(mod)
        return CPSTransformerContext(subsequent_stmts=[], subsequent_live_vars=LivenessTracker(), curr_class=None,
                                     curr_func=None, global_names=global_names)

    def prepend_subsequent_stmts(self, stmts: List[ast.stmt], orig_stmt: ast.stmt) -> None:
        """
        Prepends statement `stmt` to list of subsequent statements; updates live variables using statement `orig_stmt`.

        :param stmts: subsequent statement to prepend.
        :param orig_stmt: pre-CPS-transformed version of `stmts`; must have the same effect on liveness as `stmts`.
        """
        self.subsequent_stmts[0:0] = stmts
        self.subsequent_liveness.prepend_stmt(orig_stmt)

    def enter_loop(self, loop: LoopT) -> None:
        """
        Marks entering a loop body; updates live variables.

        :param loop: AST node for the original (non-CPS-transformed) loop.
        """
        self.subsequent_stmts.insert(0, LoopBodyDelimiter(loop))
        # All live variables of the loop are live at the end of an iteration.  Assumes that CPS transformation
        # doesn't change liveness information of the loop.
        self.subsequent_liveness.prepend_stmt(loop)

    def clone(self) -> "CPSTransformerContext":
        """Clones a context."""
        return CPSTransformerContext(
            subsequent_stmts=self.subsequent_stmts[:],
            subsequent_live_vars=self.subsequent_liveness.clone(),
            curr_class=self.curr_class,
            curr_func=self.curr_func,
            global_names=set(self.global_names)
        )

    def enter_class_scope(self, curr_class: ast.ClassDef) -> "CPSTransformerContext":
        """Returns a new context with an updated current class."""
        if self.curr_class or self.curr_func:
            raise NodeNotSupportedError(curr_class, "Class decls within class/function decls not supported")

        return CPSTransformerContext(subsequent_stmts=[], subsequent_live_vars=LivenessTracker(), curr_class=curr_class,
                                     curr_func=None, global_names=set(self.global_names))

    def enter_function_scope(self, curr_func: ast.FunctionDef) -> "CPSTransformerContext":
        """Returns a new context with an updated current function and set of accessible global names."""
        if self.curr_func:
            raise NodeNotSupportedError(curr_func, "Nested functions not supported")

        new_global_names = set(self.global_names)
        # Remove global names shadowed in function.
        vars_by_usage = find_variables_by_usage(curr_func)
        new_global_names -= vars_by_usage[ast.Param]
        new_global_names -= vars_by_usage[ast.Store]

        return CPSTransformerContext(subsequent_stmts=[], subsequent_live_vars=LivenessTracker(),
                                     curr_class=self.curr_class, curr_func=curr_func, global_names=new_global_names)

    def make_continuation_class(self, cont_class_name: str, result_id: str) -> Tuple[ast.ClassDef, List[str]]:
        """
        Makes a ClassDef AST for a continuation class.  The continuation class name is added to globals.

        Generating a continuation from inside a loop body is tricky.  See comments below for details.

        :param cont_class_name: name of the continuation class.
        :param result_id: name of variable storing the function call result.
        :return: continuation class definition and a list of captured variable names.
        """
        # Don't capture the function call result or any globals.
        captured_vars = list(self.subsequent_liveness.live_vars - {result_id} - self.global_names)
        captured_vars_str = ", ".join(captured_vars)
        base_class = parse_ast_expr("rt.Continuation")
        run_method = parse_ast_stmt(f"""
            @staticmethod
            def run({result_id}, {captured_vars_str}):
                pass  # Will be replaced by method body.
        """)
        assert isinstance(run_method, ast.FunctionDef)

        cont_body: List[ast.stmt] = []
        for subsequent_stmt in self.subsequent_stmts:
            if isinstance(subsequent_stmt, LoopBodyDelimiter):
                # All statements currently in `cont_body` is part of a loop body, which ends here.
                # The loop will look like this in the continuation:
                #
                #    for _ in range(1):  # "Dummy" loop, runs only once
                #        .  -+
                #        .   |
                #        .   |- cont_body, which is part of the "actual" loop body
                #        .   |
                #        .  -+
                #    else:
                #        while ...:  -+
                #           .         |
                #           .         |- subsequent_stmt.loop, i.e., the entire actual loop
                #           .         |
                #           .        -+
                #
                # The current iteration of the loop can end in one of three ways:
                #
                #   - By reaching the end of the loop body: the dummy loop terminates, the "else" branch is run, and the
                #     actual loop restarts;
                #   - Through a "continue" statement: the rest of the dummy loop body is skipped, the "else" branch
                #     is run, and the actual loop restarts;
                #   - Through a "break" statement: both the rest of the dummy loop body and the "else" branch are
                #     skipped, so the actual loop isn't restarted.
                #
                # Exceptions aren't supported anywhere in the compiler yet so they're not considered.
                cont_body = [
                    clone_node(
                        parse_ast_stmt("for _ in range(1): pass"),
                        body=cont_body or [ast.Pass()],  # Loop body is not allowed to be empty.
                        orelse=[subsequent_stmt.loop]
                    )
                ]
            else:
                cont_body.append(subsequent_stmt)
        run_method.body = cont_body or [ast.Pass()]  # Method body is not allowed to be empty.

        self.global_names.add(cont_class_name)  # The newly created continuation class is globally accessible.

        return ast.ClassDef(
            name=cont_class_name,
            bases=[base_class],
            body=[run_method],
            decorator_list=[]
        ), captured_vars


ExtrasT = List[ast.stmt]
VisitReturnT = Tuple[Union[ast.stmt, List[ast.stmt]], ExtrasT]


class CPSTransformer(MyNodeVisitor):
    """Transforms an AST into continuation-passing style.  Produces a new AST without mutating the original.

    See the `transform_to_cps` function below for usage.
    """

    def __init__(self, ignored: Set[ast.AST]) -> None:
        self.cont_counts: DefaultDict[str, int] = defaultdict(int)
        self._ignored = ignored
        super(CPSTransformer, self).__init__()

    def transform_assign_call(self, assign: ast.Assign, ctx: CPSTransformerContext) -> VisitReturnT:
        """
        Creates a continuation and passes it to a function call.  Returns a statement making the call.

        The assignment must have the form `x = foo()`; this is enforced by the flatten pass.
        """
        call = assign.value
        assert isinstance(call, ast.Call)

        assert len(assign.targets) == 1
        target = assign.targets[0]
        assert isinstance(target, ast.Name)
        result_id = target.id

        # Otherwise, the invoked function might take a continuation, so create a continuation class.
        if not ctx.curr_func:
            # TODO(zhangwen): make sure function call at top level doesn't pause.
            return assign, []
        outer_func_name = ctx.curr_func.name
        cont_count = self.cont_counts[outer_func_name]
        self.cont_counts[outer_func_name] += 1
        cont_class_name = f"Cont_{outer_func_name}_{cont_count}"
        cont_class_def, captured_vars = ctx.make_continuation_class(cont_class_name, result_id)
        extras: ExtrasT = [cont_class_def]

        # Make an instance of the continuation class and transform the `Call`.
        captured_vars_str = ", ".join(captured_vars)
        transformed_try = parse_ast_stmt(f"""
            try:
                pass  # Original call.
            except rt.CoordinatorCall as cc__:  # Hopefully this name hasn't been used...
                cc__.add_continuation({cont_class_name}({captured_vars_str}))
                raise  # Keep unwinding the stack by re-raising the exception.
        """)
        assert isinstance(transformed_try, ast.Try)
        transformed_try.body = [assign]

        return transformed_try, extras

    def visit_stmt(self, stmt: ast.stmt, ctx: CPSTransformerContext) -> VisitReturnT:
        """Returns transformed statement(s) and list of "extra" definitions to be added to the module level."""
        if stmt in self._ignored:
            return stmt, []

        return self.visit(stmt, ctx)

    def visit_Assert(self, asr: ast.Assert, _ctx: CPSTransformerContext) -> VisitReturnT:
        return asr, []

    def visit_Assign(self, assign: ast.Assign, ctx: CPSTransformerContext) -> VisitReturnT:
        value = assign.value
        if isinstance(value, ast.Call):
            return self.transform_assign_call(assign, ctx)

        return assign, []

    def visit_AugAssign(self, aug_assign: ast.AugAssign, _ctx: CPSTransformerContext) -> VisitReturnT:
        return aug_assign, []

    def visit_Break(self, br: ast.Break, _ctx: CPSTransformerContext) -> VisitReturnT:
        return br, []

    def visit_ClassDef(self, class_def: ast.ClassDef, ctx: CPSTransformerContext) -> VisitReturnT:
        body, extras = self.visit_list(class_def.body, ctx.enter_class_scope(class_def))
        if any(kw.arg == "metaclass" for kw in class_def.keywords):
            raise NodeNotSupportedError(class_def, "Class definition with metaclass not supported")
        # Set metaclass in case __init__() pauses.
        keywords = class_def.keywords + \
                   [ast.keyword(arg="metaclass", value=parse_ast_expr("rt.TransformedClassMeta"))]
        return clone_node(class_def, body=body, keywords=keywords), extras

    def visit_Continue(self, cont_stmt: ast.Continue, _ctx: CPSTransformerContext) -> VisitReturnT:
        return cont_stmt, []

    def visit_Expr(self, _expr: ast.Expr, _ctx: CPSTransformerContext) -> "NoReturn":
        assert False, "Expr should have been wrapped in an Assign during flatten."

    def visit_If(self, if_stmt: ast.If, ctx: CPSTransformerContext) -> VisitReturnT:
        body_ctx = ctx.clone()
        body, body_extras = self.visit_list(if_stmt.body, body_ctx)
        orelse_ctx = ctx.clone()
        orelse, orelse_extras = self.visit_list(if_stmt.orelse, orelse_ctx)

        return clone_node(if_stmt, body=body, orelse=orelse), body_extras + orelse_extras

    def visit_While(self, while_stmt: ast.While, ctx: CPSTransformerContext) -> VisitReturnT:
        if while_stmt.orelse:
            raise NodeNotSupportedError(while_stmt, "While statement with orelse not supported")

        # For now `transformed_while` is just a copy of the original `While`.  Its loop body will be updated to the
        # transformed version in the end, in turn updating each continuation that refers to it.
        transformed_while = clone_node(while_stmt)

        body_ctx = ctx.clone()
        body_ctx.enter_loop(transformed_while)
        transformed_body, body_extras = self.visit_list(while_stmt.body, body_ctx)
        transformed_while.body = transformed_body
        # Now the continuation contains the transformed loop body.

        return transformed_while, body_extras

    def visit_For(self, for_stmt: ast.For, ctx: CPSTransformerContext) -> VisitReturnT:
        # The treatment here is similar to `visit_While`.
        if for_stmt.orelse:
            raise NodeNotSupportedError(for_stmt, "For statement with orelse not supported")

        transformed_for = clone_node(for_stmt)

        body_ctx = ctx.clone()
        # Because the iterable wrapper keeps state on the iteration position, it is possible to resume a for loop by
        # running the same for loop on the same wrapped iterable.
        body_ctx.enter_loop(transformed_for)
        transformed_body, body_extras = self.visit_list(for_stmt.body, body_ctx)
        transformed_for.body = transformed_body

        return transformed_for, body_extras

    def visit_FunctionDef(self, func_def: ast.FunctionDef, ctx: CPSTransformerContext) -> VisitReturnT:
        body, extras = self.visit_list(func_def.body, ctx.enter_function_scope(func_def))
        return clone_node(func_def, body=body), extras

    def visit_Pass(self, pass_stmt: ast.Pass, _ctx: CPSTransformerContext) -> VisitReturnT:
        return pass_stmt, []

    def visit_Return(self, ret: ast.Return, _ctx: CPSTransformerContext) -> VisitReturnT:
        return ret, []

    def visit_Import(self, imp: ast.Import, _ctx: CPSTransformerContext) -> VisitReturnT:
        return imp, []

    def visit_ImportFrom(self, imp_from: ast.ImportFrom, _ctx: CPSTransformerContext) -> VisitReturnT:
        return imp_from, []

    def visit_list(self,
                   stmts: List[ast.stmt],
                   ctx: CPSTransformerContext,
                   at_module_level: bool = False) -> Tuple[List[ast.stmt], ExtrasT]:
        result: List[ast.stmt] = []
        extras: ExtrasT = []
        for stmt in reversed(stmts):
            curr_result, curr_extras = self.visit_stmt(stmt, ctx)
            if isinstance(curr_result, ast.stmt):
                curr_result = [curr_result]
            result[0:0] = curr_result
            ctx.prepend_subsequent_stmts(curr_result, orig_stmt=stmt)

            if at_module_level:
                result[0:0] = curr_extras  # We're at Module level, so just insert the extras.
            else:
                extras.extend(curr_extras)

        return result, extras

    def visit_Module(self, mod: ast.Module) -> ast.Module:
        ctx = CPSTransformerContext.new_context(mod)
        body, extras = self.visit_list(mod.body, ctx, at_module_level=True)
        assert not extras, "Module body shouldn't produce any extra declarations."
        return clone_node(mod, body=body)


def transform_to_cps(mod: ast.Module, ignored: Set[ast.AST]) -> ast.Module:
    """Transforms a Module to continuation passing style.  Assumes that the Module AST is flattened."""
    transformed = CPSTransformer(ignored=ignored).visit_Module(mod)
    return transformed
