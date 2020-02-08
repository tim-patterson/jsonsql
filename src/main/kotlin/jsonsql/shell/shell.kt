package jsonsql.shell

import com.fasterxml.jackson.databind.ObjectMapper
import jsonsql.SqlLexer
import jsonsql.physical.operatorTreeFromSql
import jsonsql.functions.StringInspector
import jsonsql.physical.PhysicalTree
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.Token
import org.jline.reader.Highlighter
import org.jline.reader.LineReader
import org.jline.reader.LineReaderBuilder
import org.jline.reader.UserInterruptException
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStringBuilder
import org.jline.utils.AttributedStyle
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.util.regex.Pattern
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    // Disable stupid s3 partial stream warnings
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    /**
     * Hacky arg parsing here to support testing graal binaries
     * TODO tidy up.
     * -e for execute and j for json return
     * the next arg is the sql to run
     */
    if (args.size >= 2 && args[0] == "-ej") {
        val objectWriter = ObjectMapper().writer()
        val query = args[1]
        val tree = operatorTreeFromSql(query)
        tree.execute().use { data ->
            objectWriter.writeValue(System.out, data.toList())
        }
        println()
        exitProcess(0)
    }


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

    val commandBuffer = mutableListOf<String>()

    try {
        val currThread = Thread.currentThread()
        terminal.handle(Terminal.Signal.INT, { currThread.interrupt() })
        terminal.writer().println(AttributedString("JsonSQL", AttributedStyle.BOLD).toAnsi(terminal))
        while (true) {
            val line = lineReader.readLine("> ")
            commandBuffer.add(line)

            if (line.contains(";")) {
                val query = commandBuffer.joinToString("\n")
                commandBuffer.clear()
                try {
                    val operatorTree = operatorTreeFromSql(query)
                    renderTable(terminal, operatorTree)
                } catch (e: InterruptedException) {
                    terminal.writer().println(AttributedString("Query Cancelled", AttributedStyle.DEFAULT.foreground(AttributedStyle.RED)).toAnsi(terminal))
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
            SqlLexer.CSV to keywordStyle1,
            SqlLexer.LIMIT to keywordStyle1,
            SqlLexer.NOT to keywordStyle1,
            SqlLexer.NULL to keywordStyle1,
            SqlLexer.ORDER to keywordStyle1,
            SqlLexer.SELECT to keywordStyle1,
            SqlLexer.WHERE to keywordStyle1,
            SqlLexer.SINGLE_LINE_COMMENT to commentStyle,
            SqlLexer.LATERAL to keywordStyle1,
            SqlLexer.VIEW to keywordStyle1,
            SqlLexer.INSERT to keywordStyle1,
            SqlLexer.INTO to keywordStyle1
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

    // Unused
    override fun setErrorPattern(errorPattern: Pattern) {}
    override fun setErrorIndex(errorIndex: Int) {}
}

private val tableStyle = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)
private val headerStyle = AttributedStyle.BOLD.foreground(AttributedStyle.CYAN)


fun renderTable(terminal: Terminal, operator: PhysicalTree) {
    val startTime = System.currentTimeMillis()
    // Get the first 1000 rows to get a good guess on column width etc
    val rowBuffer = mutableListOf<List<String>>()
    val maxWidths = operator.columnAliases.map{ it.fieldName.length }.toMutableList()

    var rowCount = 0

    val bufferSize = 1000

    operator.execute().use { data ->
        val dataIter = data.iterator()

        for (i in 0 until bufferSize) {
            if (!dataIter.hasNext()) break
            val row = dataIter.next()
            val stringRow = stringifyRow(row)
            rowBuffer.add(stringRow)
            stringRow.mapIndexed { idx, cell -> maxWidths[idx] = maxOf(maxWidths[idx], cell.length) }
        }

        // render header.
        val horizontalLine = AttributedString(
                maxWidths.joinToString("+", prefix = "+", postfix = "+") { "-".repeat(it) },
                tableStyle
        ).toAnsi(terminal)

        terminal.writer().println(horizontalLine)
        terminal.writer().println(renderLine(operator.columnAliases.map { it.fieldName }, maxWidths, headerStyle).toAnsi(terminal))
        terminal.writer().println(horizontalLine)
        rowBuffer.forEach {
            terminal.writer().println(renderLine(it, maxWidths).toAnsi(terminal))
            terminal.flush()
            rowCount++
        }

        while (dataIter.hasNext()) {
            val row = dataIter.next()
            terminal.writer().println(renderLine(stringifyRow(row), maxWidths).toAnsi(terminal))
            terminal.flush()
            rowCount++
        }
        terminal.writer().println(horizontalLine)
        val totalTimeMs = System.currentTimeMillis() - startTime
        terminal.writer().println(AttributedString("$rowCount rows returned in ${totalTimeMs / 1000.0} seconds", AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN)).toAnsi(terminal))
    }
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

