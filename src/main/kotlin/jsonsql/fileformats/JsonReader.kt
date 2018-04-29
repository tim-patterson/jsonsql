package jsonsql.fileformats

import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonToken
import jsonsql.filesystems.FileSystem
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Reader

class JsonReader(val path: String) {
    private val files: Iterator<String> by lazy(::listDirs)
    private var reader: Reader? = null
    private var jsonReader: com.google.gson.stream.JsonReader? = null
    private val gson = GsonBuilder().setLenient().create()

    fun next(): Map<String,*>? {
        while(true) {
            if (reader == null){
                if (files.hasNext()) {
                    openReader(files.next())
                } else {
                    return null
                }
            }
            if (jsonReader!!.peek() != JsonToken.END_DOCUMENT) {
                val row = gson.fromJson<Map<String,*>>(jsonReader, Map::class.java)
                return row.mapKeys { it.key.toLowerCase() }
            } else {
                reader = null
            }
        }
    }

    fun close() {
        reader?.close()
    }

    private fun listDirs(): Iterator<String> {
        // Sort so the describe operator has more of a chance of getting the latest data
        return FileSystem.listDir(path).sortedDescending().iterator()
    }

    private fun openReader(path: String) {
        reader = NullStrippingReader(BufferedReader(InputStreamReader(FileSystem.open(path))))
        jsonReader = gson.newJsonReader(reader)
    }

    // Custom class to strip nul chars out of json, don't ask....
    private class NullStrippingReader(val delegate: Reader): Reader() {
        override fun close() {
            delegate.close()
        }

        override fun read(cbuf: CharArray, off: Int, len: Int): Int {
            val i = delegate.read(cbuf, off, len)
            var shuffle = 0
            for (index in off until (off + i)){
                val c = cbuf[index]
                if (c == 0.toChar()) {
                    shuffle++
                } else if (shuffle != 0) {
                    cbuf[index - shuffle] = c
                }
            }
            return i - shuffle
        }

    }
}