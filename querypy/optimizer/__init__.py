import abc

from querypy.datasources.csv import CSVDataSource
from querypy.planner.expressions import LogicalPlan, LogicalExpression
from querypy.planner.expressions.logical import Column, LiteralInteger, Gt, \
    Binary, Literal, Aggregate as AggregateExpr
from querypy.planner.plans.logical import Aggregate, Projection, Filter, Scan
from querypy.utils import get_text_tree


class OptimizerRule(abc.ABC):
    @abc.abstractmethod
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        pass


def extract_columns(expr: list[LogicalExpression],
                    input: LogicalPlan = None,
                    columns: list[str] = None):
    if not columns:
        columns = []

    for ex in expr:
        match ex:
            case Column():
                columns.append(ex.name)
            case Binary():
                extract_columns([ex.l], input, columns)
                extract_columns([ex.r], input, columns)
            case Literal():
                pass
            case AggregateExpr():
                extract_columns([ex.expr], input, columns)
            case _:
                raise NotImplementedError(f"Not supported column extraction "
                                          f"in {ex}")
    return columns


class ProjectionPushDown(OptimizerRule):
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        return self.push_down(plan)

    def push_down(self, plan: LogicalPlan, column_names: list[str] = None):
        if not column_names:
            column_names = []

        match plan:
            case Projection():
                column_names.extend(extract_columns(plan.expr,
                                                    columns=column_names))
                input = self.push_down(plan.input, column_names)
                return Projection(input, plan.expr)
            case Filter():
                column_names.extend(extract_columns([plan.expr],
                                                    columns=column_names))
                input = self.push_down(plan.input, column_names)
                return Filter(input, plan.expr)

            case Aggregate():
                column_names.extend(extract_columns(plan.group_by,
                                                    columns=column_names))
                assert isinstance(plan.aggregate, list)
                for ex in plan.aggregate:
                    column_names.extend(
                        extract_columns([ex], columns=column_names))
                input = self.push_down(plan.input, column_names)
                return Aggregate(input, plan.group_by, plan.aggregate)
            case Scan():
                column_names = list(set(column_names))
                column_names.sort()
                return Scan(plan.path, plan.datasource, column_names)
            case _:
                raise NotImplementedError(f'Plan {plan} is not supported.')
