package jsonsql.query

import jsonsql.SqlLexer
import jsonsql.SqlParser
import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNode

/**
 * All the logic that transforms the ANTLR parsetree into our Query datastructure
 */


fun parse(statement: String): Query {
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

private fun parseStmt(stmt: SqlParser.StmtContext): Query {
    return when {
        stmt.describe_stmt() != null -> parseDescribeStmt(stmt.describe_stmt())
        stmt.EXPLAIN() != null -> Query.Explain(parseSelectStmt(stmt.select_stmt()))
        stmt.insert_stmt() != null -> Query.Insert(parseSelectStmt(stmt.insert_stmt().select_stmt()), parseTable(stmt.insert_stmt().table()))
        stmt.select_stmt() != null -> parseSelectStmt(stmt.select_stmt())
        else -> malformedAntlrCtx()
    }
}

private fun parseDescribeStmt(describe: SqlParser.Describe_stmtContext): Query.Describe {
    val tableSyntax = describe.TABLE() != null
    return Query.Describe(parseTable(describe.table()), tableSyntax)
}

private fun parseSelectStmt(select: SqlParser.Select_stmtContext): Query.Select {
    val expressions = select.named_expr().map(::parseNamedExpression)
    val source = parseSource(select.source())
    val limit = select.NUMERIC_LITERAL()?.let { Integer.parseInt(it.text) }
    val predicate = select.predicate()?.let { parseExpression(select.predicate().expr()) }
    val groupBy = select.group_by()?.let { it.expr().map(::parseExpression) }
    val orderBy = select.order_by()?.let { it.order_by_expr().map(::parseOrderByExpression)}
    return Query.Select(expressions, source, predicate, groupBy, orderBy, limit)
}

private fun parseSource(source: SqlParser.SourceContext): Query.SelectSource {
    var operator = parseTableOrSubquery(source.table_or_subquery())

    source.join().forEach { join ->
        operator = Query.SelectSource.Join(operator, parseTableOrSubquery(join.table_or_subquery()), parseExpression(join.expr()))
    }
    return operator
}

private fun parseTableOrSubquery(source: SqlParser.Table_or_subqueryContext): Query.SelectSource {
    return when {
        source.table() != null -> Query.SelectSource.JustATable(parseTable(source.table()), source.IDENTIFIER()?.let { parseIdentifierStr(it) })
        source.subquery() != null -> Query.SelectSource.InlineView(parseSelectStmt(source.subquery().select_stmt()), source.IDENTIFIER()?.let { parseIdentifierStr(it) })
        source.lateral_view() != null -> Query.SelectSource.LateralView(parseTableOrSubquery(source.table_or_subquery()), parseNamedExpression(source.lateral_view().named_expr()))
        else -> malformedAntlrCtx()
    }
}

private fun parseNamedExpression(node: SqlParser.Named_exprContext): NamedExpr {
    // only 2 cases where node has an identifier, select foobar and select foobar.baz
    val label = node.IDENTIFIER()?.let { parseIdentifierStr(it) } ?: node.expr().IDENTIFIER()?.let { parseIdentifierStr(it) }
    return NamedExpr(parseExpression(node.expr()), label)
}

private fun parseOrderByExpression(node: SqlParser.Order_by_exprContext): OrderExpr {
    return OrderExpr(parseExpression(node.expr()), node.DESC() == null)
}

private fun parseExpression(node: SqlParser.ExprContext): Expression {
    return when {
        node.function_call() != null -> parseFunctionCall(node.function_call())

        node.expr().size == 1 -> {
            val expr = listOf(parseExpression(node.expr().first()))
            when {
                node.IS() != null -> {
                    val functionName = if (node.NOT() == null) "is_null" else "is_not_null"
                    Expression.Function(functionName, expr)
                }
                node.OP_DOT() != null -> Expression.Function("idx", expr + Expression.Constant(parseIdentifierStr(node.IDENTIFIER())))
                else -> expr.first() // ( expr ) case
            }
        }
        // Math operators etc
        node.expr().size == 2 -> {
            val exprs = listOf(parseExpression(node.expr()[0]), parseExpression(node.expr()[1]))
            when {
                node.OP_IDX() != null -> Expression.Function("idx", exprs)
                node.OP_PLUS() != null -> Expression.Function("add", exprs)
                node.OP_MINUS() != null -> Expression.Function("minus", exprs)
                node.OP_MULT() != null -> Expression.Function("multiply", exprs)
                node.OP_DIV() != null -> Expression.Function("divide", exprs)
                node.OP_GT() != null -> Expression.Function("gt", exprs)
                node.OP_GTE() != null -> Expression.Function("gte", exprs)
                node.OP_LT() != null -> Expression.Function("lt", exprs)
                node.OP_LTE() != null -> Expression.Function("lte", exprs)
                node.OP_EQ() != null -> Expression.Function("equal", exprs)
                node.OP_NEQ() != null -> Expression.Function("not_equal", exprs)
                node.OP_AND() != null -> Expression.Function("and", exprs)
                node.OP_OR() != null -> Expression.Function("or", exprs)
                else -> malformedAntlrCtx()
            }
        }

        node.STRING_LITERAL() != null -> parseStringLiteral(node.STRING_LITERAL() )
        node.NUMERIC_LITERAL() != null -> parseNumericLiteral(node.NUMERIC_LITERAL() )
        node.TRUE() != null -> Expression.Constant(true)
        node.FALSE() != null -> Expression.Constant(false)
        node.NULL() != null -> Expression.Constant(null)
        node.IDENTIFIER() != null -> parseIdentifier(node.IDENTIFIER())

        else -> malformedAntlrCtx()
    }
}

private fun parseIdentifier(node: TerminalNode): Expression {
    return Expression.Identifier(Field(null, parseIdentifierStr(node)))
}

private fun parseIdentifierStr(node: TerminalNode): String {
    val raw = node.text.toLowerCase()
    return if (raw.startsWith('`')) {
        raw.substring(1, raw.length -1)
    } else {
        raw
    }
}

private fun parseFunctionCall(node: SqlParser.Function_callContext): Expression.Function {
    val expressions = node.expr().map(::parseExpression)
    return Expression.Function(node.IDENTIFIER().text.toLowerCase(), expressions)
}

private fun parseStringLiteral(node: TerminalNode): Expression.Constant {
    return Expression.Constant(parseString(node))
}

private fun parseNumericLiteral(node: TerminalNode): Expression.Constant {
    return Expression.Constant(node.text.toDouble())
}

private fun parseTable(table: SqlParser.TableContext): Table {
    val tableType = when {
        table.table_type().CSV() != null -> TableType.CSV
        table.table_type().DIR() != null -> TableType.DIR
        else -> TableType.JSON
    }
    return Table(tableType, parseString(table.STRING_LITERAL()))
}

private fun parseString(node: TerminalNode): String {
    val text = node.text
    return text.substring(1, text.length -1)
}
