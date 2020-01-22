package jsonsql.integration

import jsonsql.executor.execute
import jsonsql.functions.StringInspector
import jsonsql.physical.rowSequence
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers

fun testQuery(expr: String, expected: String) {
    execute(expr).use { operatorTree ->
        val operator = operatorTree.root
        val results = operator.rowSequence()
        MatcherAssert.assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), Matchers.equalTo(expected.trim()))
    }
}