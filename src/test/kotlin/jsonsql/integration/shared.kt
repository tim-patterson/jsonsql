package jsonsql.integration

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.TypeFactory
import jsonsql.executor.operatorTreeFromSql
import jsonsql.functions.StringInspector
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers
import java.io.File

@SuppressWarnings
fun testQuery(expr: String, expected: String) {
    val results = if(System.getProperty("test.external.binary") != null) {
        // This is used to test against the binary produced by Graal as some things may not work in the native
        // image so it's good to check
        val bin = File("jsonsql").absoluteFile.path
        val process = ProcessBuilder().command(bin, "-ej", expr).redirectError(ProcessBuilder.Redirect.INHERIT).start()
        ObjectMapper().readValue(process.inputStream, object: TypeReference<List<List<Any?>>>(){})
    }else {
        operatorTreeFromSql(expr).execute().use { data ->
            data.toList()
        }
    }

    MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
}