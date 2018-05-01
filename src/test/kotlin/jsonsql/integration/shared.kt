package jsonsql.integration

import jsonsql.executor.execute
import jsonsql.functions.StringInspector
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers

fun testQuery(expr: String, expected: String) {
    val results = mutableListOf<List<Any?>>()
    val operator = execute(expr)
    while (true) {
        operator.next()?.let { results.add(it) } ?: break
    }
    operator.close()

    MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
}