package jsonsql.integration

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

import jsonsql.executor.execute

@RunWith(JUnitPlatform::class)
object AstSpec: Spek({

    describe("Basic Integration Test") {
        it("simple select") {
            testExpression("select rownum from json 'test_data/nested.json' limit 3;", listOf(
                    listOf(1.0),
                    listOf(2.0),
                    listOf(3.0)
            ))
        }

        it("lateral view") {
            testExpression("select rownum, arrayval from json 'test_data/nested.json' lateral view arrayval limit 5;", listOf(
                    listOf(1.0, "a1"),
                    listOf(1.0, "a2"),
                    listOf(1.0, "a3"),
                    listOf(2.0, "b1"),
                    listOf(2.0, "b2")
            ))
        }
    }




})

private fun testExpression(expr: String, expected: List<List<Any?>>) {
    val results = mutableListOf<List<Any?>>()
    val operator = execute(expr)
    while (true) {
        operator.next()?.let { results.add(it) } ?: break
    }
    operator.close()

    assertThat(results, equalTo(expected))
}