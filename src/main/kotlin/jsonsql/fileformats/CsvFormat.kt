package jsonsql.fileformats

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import jsonsql.filesystems.EventFileSystem
import jsonsql.filesystems.FileSystem
import jsonsql.filesystems.StreamFileSystem
import jsonsql.functions.StringInspector
import java.io.BufferedInputStream
import java.io.InputStream

object CsvFormat: FileFormat {
    override fun reader(fs: FileSystem, path: String, terminating: Boolean): FileFormat.Reader {
        return when(fs) {
            is StreamFileSystem -> StreamReader(fs, path)
            is EventFileSystem -> TODO()
        }
    }

    override fun writer(fs: FileSystem, path: String, fields: List<String>): FileFormat.Writer {
        return when(fs) {
            is StreamFileSystem -> Writer(fs, path, fields)
            is EventFileSystem -> TODO()
        }
    }

    private val baseSchema = CsvSchema.emptySchema().withHeader().withEscapeChar('\\').withNullValue("\\N")

    private class StreamReader(val fs: StreamFileSystem, val path: String): FileFormat.Reader {
        private val files: Iterator<InputStream> by lazy { fs.read(path) }
        private val objectReader = CsvMapper().readerFor(Map::class.java).with(baseSchema)
        // Init with dummy csv so we don't have to handle special cases around null etc
        private var objectsIter: MappingIterator<Map<String,String?>> = objectReader.readValues("dummy\n")

        override fun next(): Map<String, *>? {
            while (true) {
                if (objectsIter.hasNext()) {
                    val row = objectsIter.next()
                    return row.mapKeys { it.key.toLowerCase() }
                } else {
                    if (files.hasNext()) {
                        nextFile(files.next())
                    } else {
                        return null
                    }
                }
            }
        }

        override fun close() {
            objectsIter.close()
        }

        private fun nextFile(stream: InputStream) {
            val inputStream = BufferedInputStream(stream)
            objectsIter = objectReader.readValues<Map<String,String?>>(inputStream)
            objectsIter.parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)
        }
    }

    private class Writer(val fs: StreamFileSystem, val path: String, val fields: List<String>): FileFormat.Writer {
        private val objectWriter by lazy {
            val out = fs.write(path)
            val writer = CsvMapper()
                    .writer().with(CsvSchema.Builder(baseSchema)
                            .addColumns(fields, CsvSchema.ColumnType.NUMBER_OR_STRING).build()
                    )

            writer.writeValues(out)
        }

        override fun write(row: List<Any?>) {
            objectWriter.write(row.map { StringInspector.inspect(it) })
        }

        override fun close() = objectWriter.close()
    }
}