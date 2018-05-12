package jsonsql.ast

import jsonsql.SqlLexer
import jsonsql.SqlParser
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNode

sealed class Ast {
    sealed class Statement: Ast() {
        data class Explain(val select: Select): Statement()
        data class Select(val expressions: List<NamedExpr>, val source: Source, val predicate: Expression?=null, val groupBy: List<Expression>?=null, val orderBy: List<OrderExpr>?=null, val limit: Int?=null) : Statement()
        data class Describe(val tbl: Table) : Statement()
        data class Insert(val select: Select, val tbl: Table): Statement()
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

    data class Table(val type: TableType, val path: String): Ast()

    sealed class Source: Ast() {
        data class Table(val table: Ast.Table, val tableAlias: String?): Source()
        data class InlineView(val inlineView: Ast.Statement.Select, val tableAlias: String?): Source()
        data class LateralView(val source: Source, val expression: NamedExpr): Source()
        data class Join(val source1: Source, val source2: Source, val joinCondition: Expression): Source()
    }
}

data class Field(val tableAlias: String?, val fieldName: String) {
    override fun toString() = tableAlias?.let { "$it.$fieldName" } ?: fieldName
}

enum class TableType { CSV, JSON, DIR }


fun parse(statement: String): Ast.Statement {
    val ins = CharStreams.fromString(statement)
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
        stmt.insert_stmt() != null -> Ast.Statement.Insert(parseSelectStmt(stmt.insert_stmt().select_stmt()), parseTable(stmt.insert_stmt().table()))
        stmt.select_stmt() != null -> parseSelectStmt(stmt.select_stmt())
        else -> malformedAntlrCtx()
    }
}

private fun parseDescribeStmt(describe: SqlParser.Describe_stmtContext): Ast.Statement.Describe {
    return Ast.Statement.Describe(parseTable(describe.table()))
}

private fun parseSelectStmt(select: SqlParser.Select_stmtContext): Ast.Statement.Select {
    val expressions = select.named_expr().map(::parseNamedExpression)
    val source = parseSource(select.source())
    val limit = select.NUMERIC_LITERAL()?.let { Integer.parseInt(it.text) }
    val predicate = select.predicate()?.let { parseExpression(select.predicate().expr()) }
    val groupBy = select.group_by()?.let { it.expr().map(::parseExpression) }
    val orderBy = select.order_by()?.let { it.order_by_expr().map(::parseOrderByExpression)}
    return Ast.Statement.Select(expressions, source, predicate, groupBy, orderBy, limit)
}

private fun parseSource(source: SqlParser.SourceContext): Ast.Source {
    var operator = parseTableOrSubquery(source.table_or_subquery())

    source.join().forEach { join ->
        operator = Ast.Source.Join(operator, parseTableOrSubquery(join.table_or_subquery()), parseExpression(join.expr()))
    }
    return operator
}

private fun parseTableOrSubquery(source: SqlParser.Table_or_subqueryContext): Ast.Source {
    return when {
        source.table() != null -> Ast.Source.Table(parseTable(source.table()), source.IDENTIFIER()?.let { parseIdentifierStr(it) })
        source.subquery() != null -> Ast.Source.InlineView(parseSelectStmt(source.subquery().select_stmt()), source.IDENTIFIER()?.let { parseIdentifierStr(it) })
        source.lateral_view() != null -> Ast.Source.LateralView(parseTableOrSubquery(source.table_or_subquery()), parseNamedExpression(source.lateral_view().named_expr()))
        else -> malformedAntlrCtx()
    }
}

private fun parseNamedExpression(node: SqlParser.Named_exprContext): Ast.NamedExpr {
    // only 2 cases where node has an identifier, select foobar and select foobar.baz
    val label = node.IDENTIFIER()?.let { parseIdentifierStr(it) } ?: node.expr().IDENTIFIER()?.let { parseIdentifierStr(it) }
    return Ast.NamedExpr(parseExpression(node.expr()), label)
}

private fun parseOrderByExpression(node: SqlParser.Order_by_exprContext): Ast.OrderExpr {
    return Ast.OrderExpr(parseExpression(node.expr()), node.DESC() == null)
}

private fun parseExpression(node: SqlParser.ExprContext): Ast.Expression {
    return when {
        node.function_call() != null -> parseFunctionCall(node.function_call())

        node.expr().size == 1 -> {
            val expr = listOf(parseExpression(node.expr().first()))
            when {
                node.IS() != null -> {
                    val functionName = if (node.NOT() == null) "is_null" else "is_not_null"
                    Ast.Expression.Function(functionName, expr)
                }
                node.OP_DOT() != null -> Ast.Expression.Function("idx", expr + Ast.Expression.Constant(parseIdentifierStr(node.IDENTIFIER())))
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
        node.TRUE() != null -> Ast.Expression.Constant(true)
        node.FALSE() != null -> Ast.Expression.Constant(false)
        node.NULL() != null -> Ast.Expression.Constant(null)
        node.IDENTIFIER() != null -> parseIdentifier(node.IDENTIFIER())

        else -> malformedAntlrCtx()
    }
}

private fun parseIdentifier(node: TerminalNode): Ast.Expression {
    return Ast.Expression.Identifier(Field(null, parseIdentifierStr(node)))
}

private fun parseIdentifierStr(node: TerminalNode): String {
    val raw = node.text.toLowerCase()
    return if (raw.startsWith('`')) {
        raw.substring(1, raw.length -1)
    } else {
        raw
    }
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
    val tableType = if (table.table_type().CSV() != null) {
        TableType.CSV
    } else if (table.table_type().DIR() != null) {
        TableType.DIR
    } else {
        TableType.JSON
    }
    return Ast.Table(tableType, parseString(table.STRING_LITERAL()))
}

private fun parseString(node: TerminalNode): String {
    val text = node.text
    return text.substring(1, text.length -1)
}
