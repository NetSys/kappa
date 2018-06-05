import ast

from typing import List, Tuple, Union, Set

from .node_visitor import MyNodeVisitor, NodeNotSupportedError
from .util import assign, load, clone_node, parse_ast_expr, parse_ast_stmt

# Typing shorthands.
ActionsT = List[ast.stmt]
VisitExprReturnT = Tuple[ast.expr, ActionsT]  # (result expr node, actions)
VisitSliceReturnT = Tuple[ast.slice, ActionsT]


def _not_expr(expr: ast.expr) -> ast.expr:
    """Generates AST node for `not expr`."""
    return ast.UnaryOp(op=ast.Not(), operand=expr)


class Flatten(MyNodeVisitor):
    """Produces a flattened-out version of an AST.  Also applies desugaring and other transformations.

    The resulting AST is essentially in three-address code, except that some literals (e.g., list literals) and
    structured control flow (e.g., `if` and `while` blocks) are kept.

    See the `flatten_module` function below for usage.
    """

    def __init__(self, ignored: Set[ast.AST]) -> None:
        self._ignored = ignored
        self._next_symbol_id = 0

        # Stack containing loop condition actions for each loop we're in, i.e., for a loop `while __x_0:`, the
        #  statements that compute `__x_0`.  For each loop, its condition actions are added before the start of the
        #  loop, at the end of the loop body, and before each `continue` statement within the loop body.  A for-loop
        #  has no condition actions because the iterator is only created once.
        self.loop_cond_actions: List[List[ast.stmt]] = []

        super(Flatten, self).__init__()

    def next_symbol_id(self) -> str:
        """Returns a `Name` node with the next unused symbol name, to be stored into."""
        symbol_id = f"__x_{self._next_symbol_id}"
        self._next_symbol_id += 1
        return symbol_id

    # Expressions
    def visit_expr(self, expr: ast.expr) -> VisitExprReturnT:
        """The evaluation of the returned result expression node must have no side effect."""
        assert expr not in self._ignored, "Expressions cannot be ignored"
        return self.visit(expr)

    def visit_Attribute(self, attr: ast.Attribute) -> VisitExprReturnT:
        value, value_actions = self.visit_expr(attr.value)
        attr_flattened = ast.Attribute(value=value, attr=attr.attr, ctx=attr.ctx)

        ctx = attr.ctx
        if isinstance(ctx, ast.Load):
            # Store the attribute's value into a symbol.
            result_id = self.next_symbol_id()
            assign_node = assign(result_id, attr_flattened)
            return load(result_id), value_actions + [assign_node]
        elif isinstance(ctx, (ast.Store, ast.Del)):
            # Don't evaluate the attribute.
            return attr_flattened, value_actions
        else:
            raise NodeNotSupportedError(attr, "Attribute context not supported")

    def visit_BinOp(self, binop: ast.BinOp) -> VisitExprReturnT:
        # `BinOp` doesn't include boolean operators, which have their own AST type `BoolOp`.
        left, left_actions = self.visit_expr(binop.left)
        right, right_actions = self.visit_expr(binop.right)
        binop_flattened = ast.BinOp(left=left, op=binop.op, right=right)

        result_id = self.next_symbol_id()
        result_node = assign(result_id, binop_flattened)
        return load(result_id), left_actions + right_actions + [result_node]

    def visit_UnaryOp(self, unaryop: ast.UnaryOp) -> VisitExprReturnT:
        operand, operand_actions = self.visit_expr(unaryop.operand)
        unaryop_flattened = ast.UnaryOp(op=unaryop.op, operand=operand)

        result_id = self.next_symbol_id()
        result_node = assign(result_id, unaryop_flattened)
        return load(result_id), operand_actions + [result_node]

    def visit_BoolOp(self, boolop: ast.BoolOp) -> VisitExprReturnT:
        """
        Due to short-circuiting, desugars boolop into nested if-statements before being flattened.

        For example, this expression::

            x = v1 and v2 and v3

        gets desugared into::

            x = v1
            if x:            # if_test (`x` for `and`, `not x` for `or`)
                x = v2
                if x:
                    x = v3

        This statement block then gets flattened.  The strategy is similar for the `or` operation.
        """
        result_id = self.next_symbol_id()
        result_node = load(result_id)

        if_test: ast.expr
        if isinstance(boolop.op, ast.And):
            if_test = result_node
        elif isinstance(boolop.op, ast.Or):
            if_test = ast.UnaryOp(op=ast.Not(), operand=result_node)
        else:
            assert False, f"BoolOp operation not recognized: {boolop}"

        body: List[ast.stmt] = [assign(result_id, boolop.values[-1])]
        for value in reversed(boolop.values[:-1]):  # Iteratively wrap body in if-statements.
            body = [ast.If(test=if_test, body=body, orelse=[])]
            body.insert(0, assign(result_id, value))

        return load(result_id), self.visit_stmt_list(body)

    def visit_Bytes(self, b: ast.Bytes) -> VisitExprReturnT:
        return b, []

    def visit_Compare(self, cmp: ast.Compare) -> VisitExprReturnT:
        result_actions = []
        left, left_actions = self.visit_expr(cmp.left)
        result_actions.extend(left_actions)

        comparators = []
        for comparator in cmp.comparators:
            comparator, comparator_actions = self.visit_expr(comparator)
            result_actions.extend(comparator_actions)
            comparators.append(comparator)

        cmp_flattened = ast.Compare(left=left, ops=cmp.ops, comparators=comparators)

        result_id = self.next_symbol_id()
        assign_node = assign(result_id, cmp_flattened)
        return load(result_id), result_actions + [assign_node]

    def visit_Call(self, call: ast.Call) -> VisitExprReturnT:
        result_actions = []

        func, func_actions = self.visit_expr(call.func)
        result_actions.extend(func_actions)

        args = []
        for arg in call.args:
            arg_flattened, arg_actions = self.visit_expr(arg)
            result_actions.extend(arg_actions)
            args.append(arg_flattened)

        keywords = []
        for kw in call.keywords:
            kw_value_flattened, kw_value_actions = self.visit_expr(kw.value)
            result_actions.extend(kw_value_actions)
            keywords.append(ast.keyword(arg=kw.arg, value=kw_value_flattened))

        result_id = self.next_symbol_id()
        result_actions.append(assign(result_id, ast.Call(func, args, keywords)))
        return load(result_id), result_actions

    def visit_Dict(self, dic: ast.Dict) -> VisitExprReturnT:
        actions = []

        keys = []
        values = []
        for key, value in zip(dic.keys, dic.values):
            # Evaluation order determined through experimentation: value1, key1, value2, key2, ...
            value_flattened, value_actions = self.visit_expr(value)
            actions.extend(value_actions)
            values.append(value_flattened)

            key_flattened = None
            if key is not None:
                key_flattened, key_actions = self.visit_expr(key)
                actions.extend(key_actions)
            keys.append(key_flattened)

        dic_flattened = ast.Dict(keys=keys, values=values)
        return dic_flattened, actions

    def visit_ListComp(self, list_comp: ast.ListComp) -> VisitExprReturnT:
        """
        Desugars a list comprehension into nested for-loops and if-statements before flattening.

        For example, this expression::

            l = [y for x in it if cond for y in x]

        gets desugared into::

            l = []
            for x in it:
                if cond:
                    for y in x:
                        l.append(y)

        This statement block then gets flattened.
        """
        result_id = self.next_symbol_id()

        # First, desugar the comprehension into nested for-loops and if-statements.
        # Iteratively wrap `body` in for-loops and if-statements.
        body: ast.stmt = ast.Expr(clone_node(
            parse_ast_expr(f"{result_id}.append()"),
            args=[list_comp.elt]
        ))

        for comp in reversed(list_comp.generators):
            if comp.is_async:  # type: ignore # Mypy doesn't recognize the `is_async` attribute of `comprehension`.
                raise NodeNotSupportedError(comp, "Asynchronous comprehension not supported")

            for if_test in reversed(comp.ifs):
                body = ast.If(test=if_test, body=[body], orelse=[])

            body = ast.For(target=comp.target, iter=comp.iter, body=[body], orelse=[])

        # Now that we've gotten rid of the comprehension, flatten the resulting action.
        define_list = parse_ast_stmt(f"{result_id} = []")
        return load(result_id), [define_list] + self.visit_stmt(body)

    def visit_Name(self, name: ast.Name) -> VisitExprReturnT:
        return name, []

    def visit_Str(self, string: ast.Str) -> VisitExprReturnT:
        return string, []

    def visit_NameConstant(self, name_const: ast.NameConstant) -> VisitExprReturnT:
        return name_const, []

    def visit_Num(self, num: ast.Num) -> VisitExprReturnT:
        return num, []

    def _visit_sequence_literal(self, lit: Union[ast.Tuple, ast.List]) -> VisitExprReturnT:
        result_actions = []
        elts = []
        for elt in lit.elts:
            elt_flattened, elt_actions = self.visit_expr(elt)
            result_actions.extend(elt_actions)
            elts.append(elt_flattened)

        flattened = clone_node(lit, elts=elts)
        return flattened, result_actions

    def visit_Tuple(self, tup: ast.Tuple) -> VisitExprReturnT:
        return self._visit_sequence_literal(tup)

    def visit_List(self, lst: ast.List) -> VisitExprReturnT:
        return self._visit_sequence_literal(lst)

    def visit_Starred(self, starred: ast.Starred) -> VisitExprReturnT:
        value, value_actions = self.visit_expr(starred.value)
        return ast.Starred(value=value, ctx=starred.ctx), value_actions

    def visit_Subscript(self, subscript: ast.Subscript) -> VisitExprReturnT:
        value, value_actions = self.visit_expr(subscript.value)
        sl, slice_actions = self.visit_slice(subscript.slice)
        ctx = subscript.ctx
        subscript_flattened = ast.Subscript(value=value, slice=sl, ctx=ctx)
        actions = value_actions + slice_actions

        if isinstance(ctx, ast.Load):
            result_id = self.next_symbol_id()
            assign_node = assign(result_id, subscript_flattened)
            return load(result_id), actions + [assign_node]
        elif isinstance(ctx, (ast.Store, ast.Del)):
            return subscript_flattened, actions

        raise NodeNotSupportedError(subscript, "Subscript context not supported")

    def visit_expr_list(self, exprs: List[ast.expr]) -> Tuple[List[ast.expr], ActionsT]:
        exprs_flattened = []
        actions = []
        for expr in exprs:
            flattened_expr, expr_actions = self.visit_expr(expr)
            exprs_flattened.append(flattened_expr)
            actions.extend(expr_actions)
        return exprs_flattened, actions

    # Statements
    def visit_stmt(self, stmt: ast.stmt) -> ActionsT:
        if stmt in self._ignored:
            return [stmt]  # Don't flatten.

        return self.visit(stmt)

    def visit_Assert(self, asr: ast.Assert) -> ActionsT:
        test, test_actions = self.visit_expr(asr.test)
        msg_actions: ActionsT
        if asr.msg is None:
            msg, msg_actions = None, []
        else:
            msg, msg_actions = self.visit_expr(asr.msg)
        result_node = ast.Assert(test=test, msg=msg)
        return test_actions + msg_actions + [result_node]

    def visit_Assign(self, ass: ast.Assign) -> ActionsT:
        value, value_actions = self.visit_expr(ass.value)
        targets, targets_actions = self.visit_expr_list(ass.targets)
        result_node = ast.Assign(targets=targets, value=value)
        return value_actions + targets_actions + [result_node]

    def visit_AugAssign(self, aug_assign: ast.AugAssign) -> ActionsT:
        value, value_actions = self.visit_expr(aug_assign.value)
        target, target_actions = self.visit_expr(aug_assign.target)
        result_node = ast.AugAssign(target=target, op=aug_assign.op, value=value)
        return value_actions + target_actions + [result_node]

    def visit_Break(self, br: ast.Break) -> ActionsT:
        return [br]

    def visit_ClassDef(self, class_def: ast.ClassDef) -> ActionsT:
        bases, bases_actions = self.visit_expr_list(class_def.bases)

        keywords = []
        keywords_actions = []
        for kw in class_def.keywords:
            kw_value_flattened, kw_value_actions = self.visit_expr(kw.value)
            keywords_actions.extend(kw_value_actions)
            keywords.append(ast.keyword(arg=kw.arg, value=kw_value_flattened))

        body = self.visit_stmt_list(class_def.body)

        if class_def.decorator_list:
            raise NodeNotSupportedError(class_def, "ClassDef decorators not supported")

        result_node = ast.ClassDef(name=class_def.name, bases=bases, keywords=keywords, body=body,
                                   decorator_list=class_def.decorator_list)
        return bases_actions + keywords_actions + [result_node]

    def visit_Continue(self, cont_stmt: ast.Continue) -> ActionsT:
        # Before going into the next iteration, emit statements to re-evaluate the loop condition.
        return self.loop_cond_actions[-1] + [cont_stmt]

    def visit_Expr(self, expr: ast.Expr) -> ActionsT:
        _, actions = self.visit_expr(expr.value)
        return actions

    def visit_If(self, if_stmt: ast.If) -> ActionsT:
        test, test_actions = self.visit_expr(if_stmt.test)
        body = self.visit_stmt_list(if_stmt.body)
        orelse = self.visit_stmt_list(if_stmt.orelse)
        result_node = ast.If(test=test, body=body, orelse=orelse)
        return test_actions + [result_node]

    def visit_Import(self, imp: ast.Import) -> ActionsT:
        return [imp]

    def visit_ImportFrom(self, imp_from: ast.ImportFrom) -> ActionsT:
        return [imp_from]

    def visit_Return(self, ret: ast.Return) -> ActionsT:
        if ret.value is None:  # A bare `return`.
            return [ret]

        value, value_actions = self.visit_expr(ret.value)
        result_node = ast.Return(value=value)
        return value_actions + [result_node]

    def visit_While(self, while_stmt: ast.While) -> ActionsT:
        test, test_actions = self.visit_expr(while_stmt.test)

        self.loop_cond_actions.append(test_actions)
        body = self.visit_stmt_list(while_stmt.body)
        self.loop_cond_actions.pop()
        body += test_actions  # Re-compute loop condition at the end of loop body.

        if while_stmt.orelse:
            raise NodeNotSupportedError(while_stmt, "While statement orelse not supported.")
        result_node = ast.While(test=test, body=body, orelse=[])
        return test_actions + [result_node]

    def visit_For(self, for_stmt: ast.For) -> ActionsT:
        # Create the iterator explicitly.
        wrapped_iter = clone_node(parse_ast_expr("iter()"), args=[for_stmt.iter])
        for_stmt = clone_node(for_stmt, iter=wrapped_iter)

        # Actually flatten the for-loop.
        target, target_actions = self.visit_expr(for_stmt.target)
        for_iter, iter_actions = self.visit_expr(for_stmt.iter)

        self.loop_cond_actions.append([])  # For-loop has no condition actions.
        body = self.visit_stmt_list(for_stmt.body)
        self.loop_cond_actions.pop()

        if for_stmt.orelse:
            raise NodeNotSupportedError(for_stmt, "For statement orelse not supported.")
        result_node = ast.For(target=target, iter=for_iter, body=body, orelse=[])
        return target_actions + iter_actions + [result_node]

    def visit_stmt_list(self, stmts: List[ast.stmt]) -> ActionsT:
        """Flattens a block of statements."""
        result_actions = []
        for stmt in stmts:
            curr_actions = self.visit_stmt(stmt)
            result_actions.extend(curr_actions)
        return result_actions

    def visit_FunctionDef(self, func_def: ast.FunctionDef) -> ActionsT:
        body = self.visit_stmt_list(func_def.body)
        for d in func_def.decorator_list:
            # TODO(zhangwen): this is a hack to specifically allow the `on_coordinator` decorator.
            if isinstance(d, ast.Name) and d.id == "on_coordinator":  # `@on_coordinator`
                continue
            if isinstance(d, ast.Attribute) and isinstance(d.value, ast.Name) and d.value.id == "rt" \
                    and d.attr == "on_coordinator":  # `@rt.on_coordinator`
                continue
            raise NodeNotSupportedError(d, "Function decorator not supported")

        return [ast.FunctionDef(name=func_def.name, args=func_def.args, body=body,
                                decorator_list=func_def.decorator_list)]

    def visit_Pass(self, pass_stmt: ast.Pass) -> ActionsT:
        return [pass_stmt]

    # Slices
    def visit_slice(self, sl: ast.slice) -> VisitSliceReturnT:
        assert sl not in self._ignored, "Slices cannot be ignored"
        return self.visit(sl)

    def visit_Index(self, index: ast.Index) -> VisitSliceReturnT:
        value_flattened, value_actions = self.visit_expr(index.value)
        return ast.Index(value=value_flattened), value_actions

    def visit_Slice(self, sl: ast.Slice) -> VisitSliceReturnT:
        actions = []
        if sl.lower is None:
            lower_flattened = None
        else:
            lower_flattened, lower_actions = self.visit_expr(sl.lower)
            actions.extend(lower_actions)

        if sl.upper is None:
            upper_flattened = None
        else:
            upper_flattened, upper_actions = self.visit_expr(sl.upper)
            actions.extend(upper_actions)

        if sl.step is None:
            step_flattened = None
        else:
            step_flattened, step_actions = self.visit_expr(sl.step)
            actions.extend(step_actions)

        return ast.Slice(lower=lower_flattened, upper=upper_flattened, step=step_flattened), actions

    def visit_ExtSlice(self, ext_sl: ast.ExtSlice) -> VisitSliceReturnT:
        dims = []
        actions = []
        for dim in ext_sl.dims:
            dim_flattened, dim_actions = self.visit_slice(dim)
            dims.append(dim_flattened)
            actions.extend(dim_actions)
        return ast.ExtSlice(dims=dims), actions

    # Module
    def visit_Module(self, mod: ast.Module) -> ast.Module:
        body = self.visit_stmt_list(mod.body)
        return ast.Module(body=body)


def flatten_module(mod: ast.Module, ignored: Set[ast.AST]) -> ast.Module:
    return Flatten(ignored=ignored).visit_Module(mod)
