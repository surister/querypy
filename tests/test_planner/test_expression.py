from querypy.planner import Projection
from querypy.planner.expressions import ColumnarExpression


def test_columnar_expression():
    log = Projection(["field", "field2"])
    ColumnarExpression("somecolumn").to_field(log)
