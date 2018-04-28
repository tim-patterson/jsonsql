package jsonsql.fileformats

import com.google.gson.GsonBuilder
import jsonsql.filesystems.FileSystem
import java.io.BufferedReader
import java.io.InputStreamReader

class JsonReader(val path: String) {
    private val files: Iterator<String> by lazy(::listDirs)
    private var reader: BufferedReader? = null
    private val gson = GsonBuilder().create()

    fun next(): Map<String,*>? {
        while(true) {
            if (reader == null){
                if (files.hasNext()) {
                    openReader(files.next())
                } else {
                    return null
                }
            }
            // Should always have a reader here
            val line = reader!!.readLine()
            if (line != null) {

                return gson.fromJson(line.trimEnd(0.toChar()), Map::class.java).mapKeys { (it.key as String).toLowerCase() }
            } else {
                reader = null
            }
        }
    }

    fun close() {
        reader?.close()
    }

    private fun listDirs(): Iterator<String> {
        return FileSystem.listDir(path).iterator()
    }

    private fun openReader(path: String) {
        reader = BufferedReader(InputStreamReader(FileSystem.open(path)))
    }
}