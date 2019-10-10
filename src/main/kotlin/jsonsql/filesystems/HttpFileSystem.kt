package jsonsql.filesystems

import java.io.InputStream
import java.io.OutputStream
import java.net.URL


object HttpFileSystem: StreamFileSystem() {
    override fun listDir(path: String): Sequence<Map<String, Any?>> {
        return sequenceOf(mapOf("path" to path))
    }

    override fun readSingle(file: Map<String, Any?>): InputStream {
        val connection =  URL(file["path"] as String).openConnection()
        connection.setRequestProperty("User-Agent", "jsonsql")
        return connection.getInputStream()
    }

    override fun write(path: String): OutputStream {
        TODO("not implemented")
    }
}