package jsonsql.filesystems

import java.io.File
import java.io.InputStream


object LocalFileSystem: FileSystem {
    override fun listDir(authority: String, path: String): List<String> {
        // for relative dirs the url parser will parse the first path component as the authority
        val filePath = if (authority.isEmpty()) path else "$authority/$path"
        return File(filePath).walk().filter { it.isFile }.map { it.absoluteFile.path }.toList()
    }

    override fun open(authority: String, path: String): InputStream {
        return File(path).inputStream()
    }
}