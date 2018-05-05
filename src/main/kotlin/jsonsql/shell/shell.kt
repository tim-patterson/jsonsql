package jsonsql.shell

import jsonsql.SqlLexer
import jsonsql.executor.execute
import jsonsql.functions.StringInspector
import jsonsql.physical.PhysicalOperator
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.Token
import org.jline.reader.*
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStringBuilder
import org.jline.utils.AttributedStyle
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter

fun main(args: Array<String>) {
    // Disable stupid s3 partial stream warnings
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")


    val historyFile = try {
        val homedir = File(System.getProperty("user.home"))
        val jsonSqlDir = File(homedir, ".jsonsql").apply { mkdir() }
        File(jsonSqlDir, "history").path
    } catch (e: Exception) {
        System.err.println("Problem creating ~/.jsonsql dir")
        ".jsonsqlhistory"
    }

    val terminal = TerminalBuilder.builder().build()
    val history = DefaultHistory()

    val lineReader = LineReaderBuilder.builder()
            .terminal(terminal)
            .history(history)
            .highlighter(SqlHighlighter)
            .variable(LineReader.HISTORY_FILE, historyFile)
            .build()

    // install shutdown hook to write out history
    Runtime.getRuntime().addShutdownHook(Thread({ history.save() }))

    var commandBuffer = mutableListOf<String>()

    try {
        terminal.writer().println(AttributedString("JsonSQL", AttributedStyle.BOLD).toAnsi(terminal))
        while (true) {
            val line = lineReader.readLine("> ")
            commandBuffer.add(line)

            if (line.contains(";")) {
                var query = commandBuffer.joinToString("\n")
                commandBuffer.clear()
                try {
                    val operator = execute(query)
                    renderTable(terminal, operator)
                    operator.close()
                } catch (e: Exception) {
                    val stringWriter = StringWriter()
                    e.printStackTrace(PrintWriter(stringWriter))
                    terminal.writer().println(AttributedString(stringWriter.toString(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED)).toAnsi(terminal))
                }
            }
        }
    } catch (e: UserInterruptException) {
        println("Bye")
    }
}


object SqlHighlighter: Highlighter {

    private val keywordStyle1 = AttributedStyle.BOLD.foreground(AttributedStyle.CYAN)
    private val keywordStyle2 = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)
    private val literalStyle = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW)
    private val commentStyle = AttributedStyle.DEFAULT.faint()

    private val tokenStyles = mapOf(
            SqlLexer.STRING_LITERAL to literalStyle,
            SqlLexer.NUMERIC_LITERAL to literalStyle,
            SqlLexer.AS to keywordStyle2,
            SqlLexer.OP_AND to keywordStyle1,
            SqlLexer.OP_OR to keywordStyle1,
            SqlLexer.ASC to keywordStyle1,
            SqlLexer.BY to keywordStyle1,
            SqlLexer.DESC to keywordStyle1,
            SqlLexer.DESCRIBE to keywordStyle1,
            SqlLexer.EXPLAIN to keywordStyle1,
            SqlLexer.FROM to keywordStyle1,
            SqlLexer.GROUP to keywordStyle1,
            SqlLexer.IS to keywordStyle1,
            SqlLexer.JSON to keywordStyle1,
            SqlLexer.LIMIT to keywordStyle1,
            SqlLexer.NOT to keywordStyle1,
            SqlLexer.NULL to keywordStyle1,
            SqlLexer.ORDER to keywordStyle1,
            SqlLexer.SELECT to keywordStyle1,
            SqlLexer.WHERE to keywordStyle1,
            SqlLexer.SINGLE_LINE_COMMENT to commentStyle,
            SqlLexer.LATERAL to keywordStyle1,
            SqlLexer.VIEW to keywordStyle1
    )

    override fun highlight(reader: LineReader, buffer: String): AttributedString {
        val ins = CharStreams.fromString(buffer)
        val lexer = SqlLexer(ins)
        lexer.removeErrorListeners()

        val strBuilder = AttributedStringBuilder()
        var prevIdx = 0
        while (true) {
            val token = lexer.nextToken()
            if (token.type == Token.EOF) break
            strBuilder.append(AttributedString(buffer, prevIdx, token.startIndex))
            val tokenStyle = tokenStyles.getOrDefault(token.type, AttributedStyle.DEFAULT)

            strBuilder.append(AttributedString(buffer, token.startIndex, token.stopIndex +1, tokenStyle))
            prevIdx = token.stopIndex + 1
        }
        strBuilder.append(AttributedString(buffer, prevIdx, buffer.length))
        return strBuilder.toAttributedString()
    }
}

private val tableStyle = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)
private val headerStyle = AttributedStyle.BOLD.foreground(AttributedStyle.CYAN)


fun renderTable(terminal: Terminal, operator: PhysicalOperator) {
    val startTime = System.currentTimeMillis()
    // Get the first 1000 rows to get a good guess on column width etc
    val rowBuffer = mutableListOf<List<String>>()
    val maxWidths = operator.columnAliases().map{ it.fieldName.length }.toMutableList()

    var rowCount = 0

    for (i in 0 until 1000) {
        val row = operator.next()
        row ?: break
        val stringRow = stringifyRow(row)
        rowBuffer.add(stringRow)
        stringRow.mapIndexed { idx, cell -> maxWidths[idx] = maxOf(maxWidths[idx], cell.length) }
    }

    // render header.
    val horizontalLine = AttributedString(
            maxWidths.map { "-".repeat(it) }.joinToString("+", prefix = "+", postfix = "+"),
            tableStyle
    ).toAnsi(terminal)

    terminal.writer().println(horizontalLine)
    terminal.writer().println(renderLine(operator.columnAliases().map { it.fieldName }, maxWidths, headerStyle).toAnsi(terminal))
    terminal.writer().println(horizontalLine)
    rowBuffer.forEach {
        terminal.writer().println(renderLine(it, maxWidths).toAnsi(terminal))
        rowCount++
    }

    while (true) {
        val row = operator.next()
        row ?: break
        terminal.writer().println(renderLine(stringifyRow(row), maxWidths).toAnsi(terminal))
        rowCount++
    }
    terminal.writer().println(horizontalLine)
    val totalTimeMs = System.currentTimeMillis() - startTime
    terminal.writer().println(AttributedString("$rowCount rows returned in ${totalTimeMs/1000.0} seconds", AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN)).toAnsi(terminal))
}

fun renderLine(line: List<String>, widths: List<Int>, style: AttributedStyle = AttributedStyle.DEFAULT): AttributedString {
    val vertical = AttributedString("|", tableStyle)
    val contentChunks = line.mapIndexed { i, s -> AttributedString(s.padEnd(widths[i]), style) }

    val center = AttributedString.join(vertical, contentChunks)
    return AttributedString.join(AttributedString.EMPTY, listOf(vertical, center, vertical))
}

fun stringifyRow(row: List<Any?>) = row.map(::stringifyCell)

fun stringifyCell(cell: Any?): String {
    cell ?: return "NULL"
    return StringInspector.inspect(cell)!!
}

