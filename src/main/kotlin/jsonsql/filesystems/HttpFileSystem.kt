package jsonsql.filesystems

import java.io.InputStream
import java.io.OutputStream
import java.net.URL


object HttpFileSystem: StreamFileSystem() {
    override fun listDir(path: String): List<Map<String, Any?>> {
        return listOf(mapOf("path" to path))
    }

    override fun read(path: String): Iterator<InputStream> {
        val connection =  URL(path).openConnection()
        connection.setRequestProperty("User-Agent", "jsonsql")
        return listOf(connection.getInputStream()).iterator()
    }

    override fun write(path: String): OutputStream {
        TODO("not implemented")
    }
}