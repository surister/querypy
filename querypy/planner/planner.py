from querypy.exceptions import UnknownColumnError
from querypy.planner.expressions import (
    LogicalExpression,
    LogicalPlan,
    PhysicalExpression,
    PhysicalPlan,
)
from querypy.planner.expressions import logical as logical_expressions
from querypy.planner.expressions import physical as physical_expressions
from querypy.planner.expressions.logical import MathOp
from querypy.planner.plans import logical as logical_plans
from querypy.planner.plans import physical as physical_plans
from querypy.planner.plans.physical import HashAggregate
from querypy.types_ import Schema


def create_physical_expr(
    expr: LogicalExpression, input: LogicalPlan
) -> PhysicalExpression:
    match expr:
        case logical_expressions.Column():
            i = input.get_schema().get_index_by_name(expr.name)
            if i < 0:
                raise UnknownColumnError(f"{i}")
            return physical_expressions.Column(i)
        case logical_expressions.Alias():
            # Obtain the index by the original name, not the aliased.
            i = input.get_schema().get_index_by_name(expr.expr.name)
            if i < 0:
                raise UnknownColumnError(f"{i}")
            return physical_expressions.Column(i)
        case logical_expressions.LiteralString():
            return physical_expressions.LiteralString(expr.value)
        case logical_expressions.LiteralInteger():
            return physical_expressions.LiteralInteger(expr.value)
        case logical_expressions.LiteralFloat():
            return physical_expressions.LiteralFloat(expr.value)
        case logical_expressions.Boolean():
            l = create_physical_expr(expr.l, input)
            r = create_physical_expr(expr.r, input)
            match expr.name:
                case "eq":
                    return physical_expressions.Eq(l, r)
                case "gt":
                    return physical_expressions.Gt(l, r)
                case "lt":
                    return physical_expressions.Lt(l, r)
        case logical_expressions.MathExpr():
            l = create_physical_expr(expr.l, input)
            r = create_physical_expr(expr.r, input)
            match expr.name:
                case MathOp.Subtract.name:
                    return physical_expressions.Subtract(l, r)
                case MathOp.Add.name:
                    return physical_expressions.Add(l, r)
                case MathOp.Multiply.name:
                    return physical_expressions.Multiply(l, r)
                case MathOp.Divide.name:
                    return physical_expressions.Divide(l, r)

    return


def create_physical_plan(plan: LogicalPlan) -> PhysicalPlan:
    match plan:
        case logical_plans.Scan():
            return physical_plans.Scan(plan.datasource, plan.projection)

        case logical_plans.Projection():
            input = create_physical_plan(plan.input)
            projection_schema = Schema([e.to_field(plan.input) for e in plan.expr])
            projection_expr = [create_physical_expr(e, plan.input) for e in plan.expr]
            return physical_plans.Projection(input, projection_schema, projection_expr)
        case logical_plans.Filter():
            input = create_physical_plan(plan.input)
            filter_expr = create_physical_expr(plan.expr, plan.input)
            return physical_plans.Filter(input, filter_expr)
        case logical_plans.Aggregate():
            input = create_physical_plan(plan.input)
            group_expr = [
                create_physical_expr(expr, plan.input) for expr in plan.group_by
            ]
            aggr = []
            for expr in plan.aggregate:
                match expr.name:
                    case "MAX":
                        aggr.append(
                            physical_expressions.Max(
                                create_physical_expr(expr.expr, plan.input)
                            )
                        )
                    case "COUNT":
                        aggr.append(
                            physical_expressions.Count(
                                create_physical_expr(expr.expr, plan.input)
                            )
                        )
                    case "AVG":
                        aggr.append(
                            physical_expressions.Avg(
                                create_physical_expr(expr.expr, plan.input)
                            )
                        )
                    case _:
                        raise NotImplementedError()

            return HashAggregate(
                input,
                group_expr=group_expr,
                aggregate_expr=aggr,
                schema=plan.get_schema(),
            )
    return
