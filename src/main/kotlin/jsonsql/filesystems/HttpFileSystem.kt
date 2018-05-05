package jsonsql.filesystems

import java.io.InputStream
import java.io.OutputStream
import java.net.URL


object HttpFileSystem: FileSystem {
    override fun listDir(path: String): List<String> {
        return listOf(path)
    }

    override fun read(path: String): InputStream {
        val connection =  URL(path).openConnection()
        connection.setRequestProperty("User-Agent", "jsonsql")
        return connection.getInputStream()
    }

    override fun write(path: String): OutputStream {
        TODO("not implemented")
    }
}