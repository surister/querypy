import sqlglot
from sqlglot.expressions import Day, DType

from querypy.datasources.csv import CSVDataSource
from querypy.planner.expressions.logical import *
from querypy.planner.plans.logical import *


def get_table_identifier(expr: sqlglot.Expr):
    parts = expr.args['from_'].args['this'].parts
    has_schema = len(parts) == 2

    schema = parts[0] if has_schema else None
    table = parts[1] if has_schema else parts[0]

    return schema, table


def create_logical_expr(expr: sqlglot.Expr):
    match expr:
        case sqlglot.expressions.core.Column():
            return Column(expr.name)

        case sqlglot.expressions.core.Alias():
            return Alias(expr.alias, create_logical_expr(expr.args['this']))

        case sqlglot.expressions.Literal():
            value = expr.args['this']
            is_string = expr.args['is_string']
            return LiteralString(value) if is_string else LiteralInteger(value)

        case sqlglot.expressions.Paren():
            return create_logical_expr(expr.args['this'])

        case sqlglot.expressions.Avg():
            return Avg(create_logical_expr(expr.args['this']))

        case sqlglot.expressions.Sum():
            return Sum(create_logical_expr(expr.args['this']))

        case sqlglot.expressions.Mul():
            return Multiply(
                create_logical_expr(expr.left),
                create_logical_expr(expr.right)
            )

        case sqlglot.expressions.Sub():
            return Subtract(
                create_logical_expr(expr.left),
                create_logical_expr(expr.right)
            )

        case sqlglot.expressions.Add():
            return Add(
                create_logical_expr(expr.left),
                create_logical_expr(expr.right)
            )

        case sqlglot.expressions.Count():
            return Count(
                create_logical_expr(expr.this)
            )

        case sqlglot.expressions.Star():
            return Column('*')

        case sqlglot.expressions.datatypes.Interval():
            _type = None
            match expr.unit.this:
                case 'DAY':
                    _type = IntervalType.DAY
                case _:
                    print(f"dont know intervaltype {type(expr.unit)}", Day())
            return LiteralInterval(expr.this.this, _type=_type)
        case sqlglot.expressions.LTE():
            return LtEq(
                create_logical_expr(expr.left),
                create_logical_expr(expr.right)
            )
        case sqlglot.expressions.Cast():
            match (expr.args['this'], expr.args['to'].this):
                case (sqlglot.expressions.Literal(), DType.DATE):
                    return LiteralDate(expr.args['this'].this)
                case _:
                    print(f'uknown sit {expr.args['to'].this.value} '
                          f'{type(expr.args['this'])}')
        case sqlglot.expressions.Ordered():
            desc = expr.args['desc']
            if desc is None:
                # DESC is the default value.
                desc = True
            return create_logical_expr(expr.this), desc
        case _:
            print('cant print, ', expr)


def create_logical_plan(expr: sqlglot.Expr):
    match expr:
        case sqlglot.expressions.query.Select():
            # These can be either alias or column, from aggregates as well.
            # so avg_#mycol can be there.
            column_expressions = [create_logical_expr(e)
                                  for e in expr.expressions]

            schema, name = get_table_identifier(expr)

            # from contains scan and projection information.
            scan = Scan(name, CSVDataSource('./.gitignore'))
            where = Filter(scan,
                           create_logical_expr(expr.args['where'].args[
                                                   'this'])) if expr.args[
                'where'] else None
            aggregate = Aggregate(
                where or scan,
                group_by=[
                    create_logical_expr(e) for e in expr.args[
                        'group'].expressions
                ],
                aggregate_functions=[
                    # Get either an AggregateFunction or an AggregateFunction
                    # being Aliased
                    *list(filter(lambda e: isinstance(e, AggregateFunction) or
                                     isinstance(e, Alias) and isinstance(e.expr,
                                                                         AggregateFunction),column_expressions))
                ]
            )

            # Projection is always second_last
            projection = Projection(input=aggregate or scan,
                                    expr=column_expressions)

            order_by = OrderBy(projection, [create_logical_expr(e) for e in
                                            expr.args['order'].expressions])


            # table_name = r.args['from_'].args['this'].parts[1].name

            return order_by
