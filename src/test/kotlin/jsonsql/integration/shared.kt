package jsonsql.integration

import jsonsql.executor.operatorTreeFromSql
import jsonsql.functions.StringInspector
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers

fun testQuery(expr: String, expected: String) {
    operatorTreeFromSql(expr).execute().use { data ->
        val results = data.toList()
        MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
    }
}