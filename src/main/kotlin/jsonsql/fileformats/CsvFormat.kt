package jsonsql.fileformats

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import jsonsql.filesystems.FileSystem
import java.io.BufferedInputStream

object CsvFormat: FileFormat {
    override fun reader(path: String): FileFormat.Reader = JacksonReader(path)
    override fun writer(path: String) = TODO("not implemented")

    private class JacksonReader(val path: String): FileFormat.Reader {
        private val files: Iterator<String> by lazy(::listDirs)
        private val objectReader = CsvMapper().readerFor(Map::class.java).with(
                CsvSchema.emptySchema().withHeader().withEscapeChar('\\').withNullValue("\\N")
        )
        // Init with dummy csv so we don't have to handle special cases around null etc
        private var objectsIter: MappingIterator<Map<String,String?>> = objectReader.readValues("dummy\n")

        override fun next(): Map<String, *>? {
            while (true) {
                if (objectsIter.hasNext()) {
                    val row = objectsIter.next()
                    return row.mapKeys { it.key.toLowerCase() }
                } else {
                    if (files.hasNext()) {
                        nextFile(files.next())
                    } else {
                        return null
                    }
                }
            }
        }

        override fun close() {
            objectsIter.close()
        }

        private fun listDirs(): Iterator<String> {
            // Sort so the describe operator has more of a chance of getting the latest data
            return FileSystem.listDir(path).sortedDescending().iterator()
        }

        private fun nextFile(path: String) {
            val inputStream = BufferedInputStream(FileSystem.read(path))
            objectsIter = objectReader.readValues<Map<String,String?>>(inputStream)
            objectsIter.parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)
        }
    }
}