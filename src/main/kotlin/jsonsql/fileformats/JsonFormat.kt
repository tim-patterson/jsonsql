package jsonsql.fileformats

import com.fasterxml.jackson.databind.ObjectMapper
import jsonsql.filesystems.FileSystem
import java.io.BufferedInputStream

object JsonFormat: FileFormat {
    override fun reader(path: String): FileFormat.Reader = JacksonReader(path)
    override fun writer(path: String) = TODO("not implemented")

    private class JacksonReader(val path: String): FileFormat.Reader {
        private val files: Iterator<String> by lazy(::listDirs)
        private var inputStream: java.io.InputStream? = null
        private val objectReader = ObjectMapper().readerFor(Any::class.java)
        private var objectsIter: Iterator<Any> = listOf<Any>().iterator()

        override fun next(): Map<String, *>? {
            while (true) {
                if (objectsIter.hasNext()) {
                    val row = objectsIter.next()
                    return when(row) {
                        is Map<*,*> -> row.mapKeys { (it.key as String).toLowerCase() }
                        else -> TODO()
                    }
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
            inputStream?.close()
        }

        private fun listDirs(): Iterator<String> {
            // Sort so the describe operator has more of a chance of getting the latest data
            return FileSystem.listDir(path).sortedDescending().iterator()
        }

        private fun nextFile(path: String) {
            inputStream = NullStrippingInputStream(BufferedInputStream(FileSystem.read(path)))
            objectsIter = objectReader.readValues<Any>(inputStream)
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