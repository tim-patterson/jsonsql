package jsonsql.fileformats

import jsonsql.filesystems.FileSystem

object DirFormat: FileFormat {
    override fun reader(fs: FileSystem, path: String, terminating: Boolean): FileFormat.Reader = DirReader(fs, path)

    override fun writer(fs: FileSystem, path: String, fields: List<String>): FileFormat.Writer = TODO()

    override fun split(): Boolean = false


    private class DirReader(val fs: FileSystem, val path: String): FileFormat.Reader {
        private val fileIter: Iterator<Map<String, Any?>> by lazy { fs.listDir(path).iterator() }

        override fun next(): Map<String, *>? {
            return if (fileIter.hasNext()) {
                fileIter.next()
            } else {
                null
            }
        }

        override fun close() {}
    }

}