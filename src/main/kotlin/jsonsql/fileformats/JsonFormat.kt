package jsonsql.fileformats

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.ObjectMapper
import jsonsql.filesystems.EventFileSystem
import jsonsql.filesystems.FileSystem
import jsonsql.filesystems.StreamFileSystem
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.OutputStream

object JsonFormat: FileFormat {
    override fun reader(fs: FileSystem, path: String, terminating: Boolean): FileFormat.Reader {
        return when(fs) {
            is StreamFileSystem -> StreamReader(fs, path)
            is EventFileSystem -> EventReader(fs, path, terminating)
        }
    }

    override fun writer(fs: FileSystem, path: String, fields: List<String>): FileFormat.Writer {
        return when(fs) {
            is StreamFileSystem -> Writer(fs, path, fields)
            is EventFileSystem -> TODO()
        }
    }

    private class StreamReader(val fs: StreamFileSystem, val path: String): FileFormat.Reader {
        private val files: Iterator<InputStream> by lazy { fs.read(path) }
        private val objectReader = ObjectMapper().readerFor(Map::class.java)
        private var jsonParser: JsonParser = JsonFactory().createParser("")

        override fun next(): Map<String, *>? {
            while (true) {
                // Skip over top level arrays etc
                while(jsonParser.nextToken() in arrayOf(JsonToken.START_ARRAY, JsonToken.END_ARRAY)) {}

                if (jsonParser.currentToken != null) {
                    val row = objectReader.readValue<Map<String,Any?>>(jsonParser)
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
            jsonParser.close()
        }

        private fun nextFile(rawInputStream: InputStream) {
            jsonParser.close()
            val inputStream = NullStrippingInputStream(BufferedInputStream(rawInputStream))
            jsonParser = JsonFactory().createParser(inputStream).configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)

        }
    }

    private class EventReader(val fs: EventFileSystem, val path: String, terminating: Boolean): FileFormat.Reader {
        private val eventReader: EventFileSystem.EventReader by lazy { fs.read(path, terminating) }
        private val jsonReader = ObjectMapper().readerFor(Map::class.java)

        override fun next(): Map<String, *>? {
            val bytes = eventReader.next()
            bytes ?: return null
            return jsonReader.readValue<Map<String, Any?>>(bytes).mapKeys { it.key.toLowerCase() }
        }

        override fun close() = eventReader.close()
    }

    private class Writer(val fs: StreamFileSystem, val path: String, val fields: List<String>): FileFormat.Writer {
        private val out: OutputStream by lazy { fs.write(path) }
        private val objectWriter = ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET).writer()

        override fun write(row: List<Any?>) {
            objectWriter.writeValue(out, fields.zip(row).toMap())
            out.write('\n'.toInt())
        }

        override fun close() = out.close()
    }

    // Custom class to strip nul chars out of json, don't ask....
    private class NullStrippingInputStream(val delegate: java.io.InputStream) : java.io.InputStream() {
        override fun read(): Int {
            while (true) {
                val r = read()
                if (r != 0) return r
            }
        }

        override fun close() {
            delegate.close()
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            val i = delegate.read(b, off, len)
            var shuffle = 0
            for (index in off until (off + i)) {
                val c = b[index]
                if (c == 0.toByte()) {
                    shuffle++
                } else if (shuffle != 0) {
                    b[index - shuffle] = c
                }
            }
            return i - shuffle
        }

    }
}