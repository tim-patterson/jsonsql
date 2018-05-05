package jsonsql.filesystems

import java.io.InputStream
import java.net.URL


object HttpFileSystem: FileSystem {
    override fun listDir(path: String): List<String> {
        return listOf(path)
    }

    override fun open(path: String): InputStream {
        val connection =  URL(path).openConnection()
        connection.setRequestProperty("User-Agent", "jsonsql")
        return connection.getInputStream()
    }

}