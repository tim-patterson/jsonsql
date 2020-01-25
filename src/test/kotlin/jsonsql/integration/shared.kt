package jsonsql.integration

import com.fasterxml.jackson.databind.ObjectMapper
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
        val process = Runtime.getRuntime().exec(arrayOf(bin, "-ej", expr))
        ObjectMapper().readValue(process.inputStream, List::class.java) as List<List<Any?>>
    }else {
        operatorTreeFromSql(expr).execute().use { data ->
            data.toList()
        }
    }

    MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
}