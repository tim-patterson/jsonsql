package jsonsql.fileformats

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.ObjectMapper
import jsonsql.filesystems.FileSystem
import java.io.BufferedInputStream

object JsonFormat: FileFormat {
    override fun reader(path: String): FileFormat.Reader = JacksonReader(path)
    override fun writer(path: String) = TODO("not implemented")

    private class JacksonReader(val path: String): FileFormat.Reader {
        private val files: Iterator<String> by lazy(::listDirs)
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

        private fun listDirs(): Iterator<String> {
            // Sort so the describe operator has more of a chance of getting the latest data
            return FileSystem.listDir(path).sortedDescending().iterator()
        }

        private fun nextFile(path: String) {
            jsonParser.close()
            val inputStream = NullStrippingInputStream(BufferedInputStream(FileSystem.read(path)))
            jsonParser = JsonFactory().createParser(inputStream).configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)

        }
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