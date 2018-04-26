package lambdadb.fileformats

import com.google.gson.Gson
import lambdadb.filesystems.FileSystem
import java.io.BufferedReader
import java.io.InputStreamReader


class JsonReader(val glob: String) {
    private val reader: BufferedReader by lazy(::openReader)
    private val gson = Gson()

    fun next(): Map<String,*>? {
        val line = reader.readLine()
        line ?: return null

        return gson.fromJson(line.trimEnd(0.toChar()), Map::class.java).mapKeys { (it.key as String).toLowerCase() }
    }

    fun close() = reader.close()


    private fun openReader(): BufferedReader {
        return BufferedReader(InputStreamReader(FileSystem.open(glob)))
    }
}