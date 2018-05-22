package jsonsql.logical

import jsonsql.ast.Ast

/**
 * Class to visit all logical operators and expressions, builds up copy of tree as
 * it goes, can be used to build modified copy of tree for optimizations etc
 */
abstract class LogicalVisitor<C> {
    open fun visit(tree: LogicalTree, context: C): LogicalTree =
            tree.copy(root = visit(tree.root, context))

    open fun visit(operator: LogicalOperator, context: C): LogicalOperator =
            when(operator) {
                is LogicalOperator.Limit -> visit(operator, context)
                is LogicalOperator.Sort -> visit(operator, context)
                is LogicalOperator.Describe -> visit(operator, context)
                is LogicalOperator.DataSource -> visit(operator, context)
                is LogicalOperator.Explain -> visit(operator, context)
                is LogicalOperator.Project -> visit(operator, context)
                is LogicalOperator.Filter -> visit(operator, context)
                is LogicalOperator.LateralView -> visit(operator, context)
                is LogicalOperator.Join -> visit(operator, context)
                is LogicalOperator.GroupBy -> visit(operator, context)
                is LogicalOperator.Gather -> visit(operator, context)
                is LogicalOperator.Write -> visit(operator, context)
            }

    open fun visit(operator: LogicalOperator.Limit, context: C): LogicalOperator = operator.copy(sourceOperator=visit(operator.sourceOperator, context))
    open fun visit(operator: LogicalOperator.Sort, context: C): LogicalOperator =
            operator.copy(
                    sortExpressions = operator.sortExpressions.map { visit(it, operator, context) },
                    sourceOperator = visit(operator.sourceOperator, context)
            )

    open fun visit(operator: LogicalOperator.Describe, context: C): LogicalOperator =
            operator.copy(tableDefinition = visit(operator.tableDefinition, context))

    open fun visit(operator: LogicalOperator.DataSource, context: C): LogicalOperator = operator.copy(tableDefinition = visit(operator.tableDefinition, context))
    open fun visit(operator: LogicalOperator.Explain, context: C): LogicalOperator = operator.copy(sourceOperator = visit(operator.sourceOperator, context))
    open fun visit(operator: LogicalOperator.Project, context: C): LogicalOperator =
            operator.copy(
                    expressions = operator.expressions.mapIndexed { index, it -> visit(it, index, operator, context) },
                    sourceOperator = visit(operator.sourceOperator, context)
            )

    open fun visit(operator: LogicalOperator.Filter, context: C): LogicalOperator =
            operator.copy(
                    predicate = visit(operator.predicate, operator, context),
                    sourceOperator = visit(operator.sourceOperator, context)
            )

    open fun visit(operator: LogicalOperator.LateralView, context: C): LogicalOperator =
            operator.copy(
                    expression = visit(operator.expression, 0, operator, context),
                    sourceOperator = visit(operator.sourceOperator, context)
            )

    open fun visit(operator: LogicalOperator.Join, context: C): LogicalOperator =
            operator.copy(
                    sourceOperator1 = visit(operator.sourceOperator1, context),
                    sourceOperator2 = visit(operator.sourceOperator2, context),
                    onClause = visit(operator.onClause, operator, context)
            )

    open fun visit(operator: LogicalOperator.GroupBy, context: C): LogicalOperator =
            operator.copy(
                    sourceOperator = visit(operator.sourceOperator, context),
                    expressions = operator.expressions.mapIndexed { index, it -> visit(it, index, operator, context) },
                    groupByExpressions = operator.groupByExpressions.map { visit(it, operator, context) }
            )

    open fun visit(operator: LogicalOperator.Gather, context: C): LogicalOperator =
            operator.copy(sourceOperator = visit(operator.sourceOperator, context))

    open fun visit(operator: LogicalOperator.Write, context: C): LogicalOperator =
            operator.copy(
                    tableDefinition = visit(operator.tableDefinition, context),
                    sourceOperator = visit(operator.sourceOperator, context)
            )

    open fun visit(namedExpression: Ast.NamedExpr, index: Int, operator: LogicalOperator, context: C): Ast.NamedExpr =
        namedExpression.copy(expression = visit(namedExpression.expression, operator, context))

    open fun visit(orderExpression: Ast.OrderExpr, operator: LogicalOperator, context: C): Ast.OrderExpr =
        orderExpression.copy(expression = visit(orderExpression.expression, operator, context))

    open fun visit(expression: Ast.Expression, operator: LogicalOperator, context: C): Ast.Expression =
        when(expression) {
            is Ast.Expression.Identifier -> visit(expression, operator, context)
            is Ast.Expression.Constant -> visit(expression, operator, context)
            is Ast.Expression.Function -> visit(expression, operator, context)
        }

    open fun visit(expression: Ast.Expression.Identifier, operator: LogicalOperator, context: C): Ast.Expression = expression
    open fun visit(expression: Ast.Expression.Constant, operator: LogicalOperator, context: C): Ast.Expression = expression
    open fun visit(expression: Ast.Expression.Function, operator: LogicalOperator, context: C): Ast.Expression =
        expression.copy(parameters = expression.parameters.map { visit(it, operator, context) })

    open fun visit(table: Ast.Table, context: C): Ast.Table = table
}