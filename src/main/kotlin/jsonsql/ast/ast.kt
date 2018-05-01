package jsonsql.ast

import jsonsql.SqlLexer
import jsonsql.SqlParser
import org.antlr.v4.runtime.ANTLRInputStream
import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNode

sealed class Ast {
    sealed class Statement: Ast() {
        data class Explain(val select: Select): Statement()
        data class Select(val expressions: List<NamedExpr>, val source: Source, val predicate: Expression?=null, val groupBy: List<Expression>?=null, val orderBy: List<OrderExpr>?=null, val limit: Int?=null) : Statement()
        data class Describe(val tbl: Table) : Statement()
    }

    data class NamedExpr(val expression: Expression, val alias: String?): Ast()
    data class OrderExpr(val expression: Expression, val asc: Boolean): Ast()

    sealed class Expression: Ast() {
        data class Function(val functionName: String, val parameters: List<Expression>) : Expression()
        data class Constant(val value: Any?) : Expression()
        // The table alias here will always be null coming out of the parse function as we can't
        // tell the difference between a table_alias.field vs a field.subfield until we've done
        // some semantic analysis in the logical phase of query planning
        data class Identifier(val field: Field): Expression()
    }

    data class Table(val path: String): Ast()

    sealed class Source: Ast() {
        data class Table(val table: Ast.Table, val tableAlias: String?): Source()
        data class InlineView(val inlineView: Ast.Statement.Select, val tableAlias: String?): Source()
        data class LateralView(val source: Source, val expression: NamedExpr): Source()
    }
}

data class Field(val tableAlias: String?, val fieldName: String) {
    override fun toString() = tableAlias?.let { "$it.$fieldName" } ?: fieldName
}


fun parse(statement: String): Ast.Statement {
    val ins = ANTLRInputStream(statement)
    val lexer = SqlLexer(ins)
    val tokens = CommonTokenStream(lexer)
    val parser = SqlParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowingErrorListener)
    return parseStmt(parser.stmt())
}

private object ThrowingErrorListener : BaseErrorListener() {
    override fun syntaxError(recognizer: org.antlr.v4.runtime.Recognizer<*, *>?, offendingSymbol: Any, line: Int, charPositionInLine: Int, msg: String, e: RecognitionException?) {
        throw ParseCancellationException("line $line:$charPositionInLine $msg")
    }
}


private fun malformedAntlrCtx(): Nothing = TODO("We should never get here!")

private fun parseStmt(stmt: SqlParser.StmtContext): Ast.Statement {
    return when {
        stmt.describe_stmt() != null -> parseDescribeStmt(stmt.describe_stmt())
        stmt.EXPLAIN() != null -> Ast.Statement.Explain(parseSelectStmt(stmt.select_stmt()))
        stmt.select_stmt() != null -> parseSelectStmt(stmt.select_stmt())
        else -> malformedAntlrCtx()
    }
}

private fun parseDescribeStmt(describe: SqlParser.Describe_stmtContext): Ast.Statement.Describe {
    return Ast.Statement.Describe(parseTable(describe.table()))
}

private fun parseSelectStmt(select: SqlParser.Select_stmtContext): Ast.Statement.Select {
    val expressions = select.named_expr().map(::parseNamedExpression)
    val source = parseSource(select.table_or_subquery())
    val limit = select.NUMERIC_LITERAL()?.let { Integer.parseInt(it.text) }
    val predicate = select.predicate()?.let { parseExpression(select.predicate().expr()) }
    val groupBy = select.group_by()?.let { it.expr().map(::parseExpression) }
    val orderBy = select.order_by()?.let { it.order_by_expr().map(::parseOrderByExpression)}
    return Ast.Statement.Select(expressions, source, predicate, groupBy, orderBy, limit)
}

private fun parseSource(source: SqlParser.Table_or_subqueryContext): Ast.Source {
    return when {
        source.table() != null -> Ast.Source.Table(parseTable(source.table()), source.IDENTIFIER()?.text?.toLowerCase())
        source.subquery() != null -> Ast.Source.InlineView(parseSelectStmt(source.subquery().select_stmt()), source.IDENTIFIER()?.text?.toLowerCase())
        source.lateral_view() != null -> Ast.Source.LateralView(parseSource(source.table_or_subquery()), parseNamedExpression(source.lateral_view().named_expr()))
        else -> malformedAntlrCtx()
    }
}

private fun parseNamedExpression(node: SqlParser.Named_exprContext): Ast.NamedExpr {
    val label = node.IDENTIFIER()?.text
    return Ast.NamedExpr(parseExpression(node.expr()), label)
}

private fun parseOrderByExpression(node: SqlParser.Order_by_exprContext): Ast.OrderExpr {
    return Ast.OrderExpr(parseExpression(node.expr()), node.DESC() == null)
}

private fun parseExpression(node: SqlParser.ExprContext): Ast.Expression {
    return when {
        node.expr().size == 1 -> {
            val expr = listOf(parseExpression(node.expr().first()))
            when {
                node.IS() != null -> {
                    val functionName = if (node.NOT() == null) "is_null" else "is_not_null"
                    Ast.Expression.Function(functionName, expr)
                }
                node.OP_DOT() != null -> Ast.Expression.Function("idx", expr + Ast.Expression.Constant(node.IDENTIFIER().text.toLowerCase()))
                else -> expr.first() // ( expr ) case
            }
        }
        // Math operators etc
        node.expr().size == 2 -> {
            val exprs = listOf(parseExpression(node.expr()[0]), parseExpression(node.expr()[1]))
            when {
                node.OP_IDX() != null -> Ast.Expression.Function("idx", exprs)
                node.OP_PLUS() != null -> Ast.Expression.Function("add", exprs)
                node.OP_MINUS() != null -> Ast.Expression.Function("minus", exprs)
                node.OP_MULT() != null -> Ast.Expression.Function("multiply", exprs)
                node.OP_DIV() != null -> Ast.Expression.Function("divide", exprs)
                node.OP_GT() != null -> Ast.Expression.Function("gt", exprs)
                node.OP_GTE() != null -> Ast.Expression.Function("gte", exprs)
                node.OP_LT() != null -> Ast.Expression.Function("lt", exprs)
                node.OP_LTE() != null -> Ast.Expression.Function("lte", exprs)
                node.OP_EQ() != null -> Ast.Expression.Function("equal", exprs)
                node.OP_NEQ() != null -> Ast.Expression.Function("not_equal", exprs)
                node.OP_AND() != null -> Ast.Expression.Function("and", exprs)
                node.OP_OR() != null -> Ast.Expression.Function("or", exprs)
                else -> malformedAntlrCtx()
            }
        }

        node.STRING_LITERAL() != null -> parseStringLiteral(node.STRING_LITERAL() )
        node.NUMERIC_LITERAL() != null -> parseNumericLiteral(node.NUMERIC_LITERAL() )
        node.IDENTIFIER() != null -> parseIdentifier(node.IDENTIFIER())
        node.function_call() != null -> parseFunctionCall(node.function_call())
        else -> malformedAntlrCtx()
    }
}

private fun parseIdentifier(node: TerminalNode): Ast.Expression {
    return Ast.Expression.Identifier(Field(null,node.text.toLowerCase()))
}

private fun parseFunctionCall(node: SqlParser.Function_callContext): Ast.Expression.Function {
    val expressions = node.expr().map(::parseExpression)
    return Ast.Expression.Function(node.IDENTIFIER().text.toLowerCase(), expressions)
}

private fun parseStringLiteral(node: TerminalNode): Ast.Expression.Constant {
    return Ast.Expression.Constant(parseString(node))
}

private fun parseNumericLiteral(node: TerminalNode): Ast.Expression.Constant {
    return Ast.Expression.Constant(node.text.toDouble())
}

private fun parseTable(table: SqlParser.TableContext): Ast.Table {
    return Ast.Table(parseString(table.STRING_LITERAL()))
}

private fun parseString(node: TerminalNode): String {
    val text = node.text
    return text.substring(1, text.length -1)
}
