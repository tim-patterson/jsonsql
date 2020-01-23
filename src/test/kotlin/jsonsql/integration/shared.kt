package jsonsql.integration

import jsonsql.executor.execute
import jsonsql.functions.StringInspector
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers

fun testQuery(expr: String, expected: String) {
    val operator = execute(expr).root
    operator.data().use { data ->
        val results = data.toList()
        MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
    }
}