package jsonsql.filesystems

import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.util.zip.GZIPInputStream

sealed class FileSystem {
    // String to list of file attributes
    // each must contain a "path" attribute
    abstract fun listDir(path: String): Sequence<Map<String, Any?>>

    companion object {
        fun listDir(path: String) = from(path).listDir(path)

        fun from(path: String): FileSystem {
            val scheme = URI.create(path).scheme
            return when(scheme) {
                null -> CompressionFileSystemDecorator(LocalFileSystem)
                "file" -> CompressionFileSystemDecorator(LocalFileSystem)
                "s3" -> CompressionFileSystemDecorator(S3FileSystem)
                "http" ->CompressionFileSystemDecorator(HttpFileSystem)
                "https" -> CompressionFileSystemDecorator(HttpFileSystem)
                "kafka" -> KafkaFileSystem
                "dummy" -> DummyFileSystem
                else -> TODO("Unknown filesystem $scheme")
            }
        }
    }
}

class CompressionFileSystemDecorator(private val fs: StreamFileSystem): StreamFileSystem() {

    override fun readSingle(file: Map<String, Any?>): InputStream {
        var path = file.getOrDefault("path", "") as String
        return if (path.endsWith(".gz") || path.endsWith(".gzip")) {
            GZIPInputStream(fs.readSingle(file))
        } else {
            fs.readSingle(file)
        }
    }

    override fun listDir(path: String) = fs.listDir(path)
    override fun write(path: String) = fs.write(path)
}

abstract class StreamFileSystem: FileSystem() {
    fun read(path: String): Iterator<InputStream> {
        return listDir(path).filter { it.getOrDefault("size",1) != 0 }.map {
            readSingle(it)
        }.iterator()
    }
    abstract fun readSingle(file: Map<String, Any?>): InputStream
    abstract fun write(path: String): OutputStream
}

abstract class EventFileSystem: FileSystem() {
    abstract fun read(path: String): EventReader
    abstract fun write(path: String): EventWriter

    interface EventReader : AutoCloseable {
        fun next(): ByteArray?
    }

    interface EventWriter : AutoCloseable {
        fun write(event: ByteArray)
    }
}

