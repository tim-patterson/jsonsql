package jsonsql.integration

import jsonsql.executor.execute
import jsonsql.functions.StringInspector
import jsonsql.physical.rowSequence
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers

fun testQuery(expr: String, expected: String) {
    execute(expr).root.use { operator ->
        val results = operator.rowSequence().map { it.values }
        MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
    }
}