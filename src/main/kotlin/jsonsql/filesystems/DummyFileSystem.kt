package jsonsql.filesystems


object DummyFileSystem: EventFileSystem() {
    override fun listDir(path: String): Sequence<Map<String, Any?>> {

        return sequenceOf(mapOf(
                    "path" to path
            ))

    }

    override fun read(path: String, terminating: Boolean): EventReader {
        var i = 0
        return object: EventReader {
            override fun next(): ByteArray? {
                Thread.sleep(1000)
                return "{\"foo\": 4, \"bar\": ${i++ % 10}}".toByteArray()
            }

            override fun close() {
            }
        }
    }

    override fun write(path: String): EventWriter {
        TODO("not implemented")
    }
}
