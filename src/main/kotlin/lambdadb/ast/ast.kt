package lambdadb.ast

import lambdadb.SqlParser
import org.antlr.v4.runtime.tree.TerminalNode

sealed class Ast {
    sealed class Statement: Ast() {
        data class Explain(val select: Select): Statement()
        data class Select(val expressions: List<NamedExpr>, val source: Source, val predicate: Expression?, val groupBy: List<Expression>?, val orderBy: List<OrderExpr>?, val limit: Int?) : Statement()
        data class Describe(val tbl: Table) : Statement()
    }

    data class NamedExpr(val expression: Expression, val alias: String?): Ast()
    data class OrderExpr(val expression: Expression, val asc: Boolean): Ast()

    sealed class Expression: Ast() {
        data class Function(val functionName: String, val parameters: List<Expression>) : Expression()
        data class Constant(val value: Any?) : Expression()
        data class Identifier(val identifier: String): Expression()
    }

    data class Table(val glob: String): Ast()

    sealed class Source: Ast() {
        data class Table(val table: Ast.Table): Source()
        data class InlineView(val inlineView: Ast.Statement.Select): Source()
    }
}



fun malformedAntlrCtx(): Nothing = TODO("We should never get here!")

fun parseStmt(stmt: SqlParser.StmtContext): Ast.Statement {
    return when {
        stmt.describe_stmt() != null -> parseDescribeStmt(stmt.describe_stmt())
        stmt.EXPLAIN() != null -> Ast.Statement.Explain(parseSelectStmt(stmt.select_stmt()))
        stmt.select_stmt() != null -> parseSelectStmt(stmt.select_stmt())
        else -> malformedAntlrCtx()
    }
}

fun parseDescribeStmt(describe: SqlParser.Describe_stmtContext): Ast.Statement.Describe {
    return Ast.Statement.Describe(parseTable(describe.table()))
}

fun parseSelectStmt(select: SqlParser.Select_stmtContext): Ast.Statement.Select {
    val expressions = select.named_expr().map(::parseNamedExpression)
    val source = parseSource(select.table_or_subquery())
    val limit = select.NUMERIC_LITERAL()?.let { Integer.parseInt(it.text) }
    val predicate = select.predicate()?.let { parseExpression(select.predicate().expr()) }
    val groupBy = select.group_by()?.let { it.expr().map(::parseExpression) }
    val orderBy = select.order_by()?.let { it.order_by_expr().map(::parseOrderByExpression)}
    return Ast.Statement.Select(expressions, source, predicate, groupBy, orderBy, limit)
}

fun parseSource(source: SqlParser.Table_or_subqueryContext): Ast.Source {
    return when {
        source.table() != null -> Ast.Source.Table(parseTable(source.table()))
        source.subquery() != null -> Ast.Source.InlineView(parseSelectStmt(source.subquery().select_stmt()))
        else -> malformedAntlrCtx()
    }
}

fun parseNamedExpression(node: SqlParser.Named_exprContext): Ast.NamedExpr {
    val label = node.IDENTIFIER()?.text
    return Ast.NamedExpr(parseExpression(node.expr()), label)
}

fun parseOrderByExpression(node: SqlParser.Order_by_exprContext): Ast.OrderExpr {
    return Ast.OrderExpr(parseExpression(node.expr()), node.DESC() == null)
}

fun parseExpression(node: SqlParser.ExprContext): Ast.Expression {
    return when {
        node.IS() != null -> {
            val functionName = if(node.NOT() == null) "is_null" else "is_not_null"
            Ast.Expression.Function(functionName, listOf(parseExpression(node.expr())))
        }
        node.STRING_LITERAL() != null -> parseStringLiteral(node.STRING_LITERAL() )
        node.NUMERIC_LITERAL() != null -> parseNumericLiteral(node.NUMERIC_LITERAL() )
        node.IDENTIFIER() != null -> parseIdentifier(node.IDENTIFIER())
        node.function_call() != null -> parseFunctionCall(node.function_call())
        else -> malformedAntlrCtx()
    }
}

fun parseIdentifier(node: TerminalNode): Ast.Expression {
    val raw = node.text.toLowerCase().split('.')
    var expression: Ast.Expression= Ast.Expression.Identifier(raw.first())
    for(part in raw.drop(1)) {
        expression = Ast.Expression.Function("idx", listOf(expression, Ast.Expression.Constant(part)))
    }
    return expression
}

fun parseFunctionCall(node: SqlParser.Function_callContext): Ast.Expression.Function {
    val expressions = node.expr().map(::parseExpression)
    return Ast.Expression.Function(node.IDENTIFIER().text.toLowerCase(), expressions)
}

fun parseStringLiteral(node: TerminalNode): Ast.Expression.Constant {
    return Ast.Expression.Constant(parseString(node))
}

fun parseNumericLiteral(node: TerminalNode): Ast.Expression.Constant {
    return Ast.Expression.Constant(node.text.toDouble())
}

fun parseTable(table: SqlParser.TableContext): Ast.Table {
    return Ast.Table(parseString(table.STRING_LITERAL()))
}

private fun parseString(node: TerminalNode): String {
    val text = node.text
    return text.substring(1, text.length -1)
}
