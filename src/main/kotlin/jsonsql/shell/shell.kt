package jsonsql.shell

import jsonsql.SqlLexer
import jsonsql.executor.execute
import jsonsql.functions.StringInspector
import jsonsql.physical.VectorizedPhysicalOperator
import jsonsql.physical.rowSequence
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.Token
import org.graalvm.nativeimage.ProcessProperties
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


fun main(args: Array<String>) {
    // Disable stupid s3 partial stream warnings
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

    // set java lib path for loading libsunec.
    try {
        val exeFile = File(ProcessProperties.getExecutableName()).absoluteFile
        val parentDir = exeFile.parentFile
        val libDir = File(parentDir, "lib")
        val currentPath = System.getProperty("java.library.path")
        val newPath = listOf(libDir.path, parentDir.path, currentPath).joinToString(File.pathSeparator)
        System.setProperty("java.library.path", newPath)
    } catch (e: NoClassDefFoundError) {
        // We expect this error when running on the jvm
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
                var root: VectorizedPhysicalOperator? = null
                try {
                    val operatorTree = execute(query)
                    root = operatorTree.root
                    renderTable(terminal, root, operatorTree.streaming)
                } catch (e: InterruptedException) {
                    terminal.writer().println(AttributedString("Query Cancelled", AttributedStyle.DEFAULT.foreground(AttributedStyle.RED)).toAnsi(terminal))
                } catch (e: Exception) {
                    val stringWriter = StringWriter()
                    e.printStackTrace(PrintWriter(stringWriter))
                    terminal.writer().println(AttributedString(stringWriter.toString(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED)).toAnsi(terminal))
                } finally {
                    root?.close()
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

/**
 * TODO there will be cases where the schema changes, in this case we really should "reserve" slots for each field name
 * as we come across them
 */
fun renderTable(terminal: Terminal, operator: VectorizedPhysicalOperator, streaming: Boolean) {
    val startTime = System.currentTimeMillis()
    // Get the first 1000 rows to get a good guess on column width etc
    val rowBuffer = mutableListOf<List<String>>()
    val maxWidths = operator.columnAliases().map{ it.fieldName.length }.toMutableList()

    var rowCount = 0

    val bufferSize = if(streaming) 1 else 1000

    val rowIter = operator.rowSequence().iterator()

    for (i in 0 until bufferSize) {
        if (rowIter.hasNext()) {
            val row = rowIter.next()
            val stringRow = stringifyRow(row)
            rowBuffer.add(stringRow)
            stringRow.mapIndexed { idx, cell -> maxWidths[idx] = maxOf(maxWidths[idx], cell.length) }
        } else {
            break
        }
    }

    // render header.
    val horizontalLine = AttributedString(
            maxWidths.joinToString("+", prefix = "+", postfix = "+") { "-".repeat(it) },
            tableStyle
    ).toAnsi(terminal)

    terminal.writer().println(horizontalLine)
    terminal.writer().println(renderLine(operator.columnAliases().map { it.fieldName }, maxWidths, headerStyle).toAnsi(terminal))
    terminal.writer().println(horizontalLine)
    rowBuffer.forEach {
        terminal.writer().println(renderLine(it, maxWidths).toAnsi(terminal))
        terminal.flush()
        rowCount++
    }

    rowIter.forEach { row ->
        terminal.writer().println(renderLine(stringifyRow(row), maxWidths).toAnsi(terminal))
        terminal.flush()
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

fun stringifyRow(row: Map<String, Any?>) = row.values.map(::stringifyCell)

fun stringifyCell(cell: Any?): String {
    cell ?: return "NULL"
    return StringInspector.inspect(cell)!!
}

