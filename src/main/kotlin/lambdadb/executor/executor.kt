package lambdadb.executor

import lambdadb.SqlLexer
import lambdadb.SqlParser
import lambdadb.ast.parseStmt
import lambdadb.physical.Operator
import lambdadb.physical.buildOperatorTree
import org.antlr.v4.runtime.ANTLRInputStream
import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.misc.ParseCancellationException


fun execute(query: String): Operator {
    val ins = ANTLRInputStream(query)
    val lexer = SqlLexer(ins)
    val tokens = CommonTokenStream(lexer)
    val parser = SqlParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowingErrorListener)
    val stmt = parseStmt(parser.stmt())

    val operatorTree = buildOperatorTree(stmt)
    operatorTree.compile()
    return operatorTree
}

private object ThrowingErrorListener : BaseErrorListener() {
    override fun syntaxError(recognizer: org.antlr.v4.runtime.Recognizer<*, *>?, offendingSymbol: Any, line: Int, charPositionInLine: Int, msg: String, e: RecognitionException?) {
        throw ParseCancellationException("line $line:$charPositionInLine $msg")
    }
}