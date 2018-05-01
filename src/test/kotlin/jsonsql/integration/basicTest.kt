package jsonsql.integration

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

import jsonsql.executor.execute
import jsonsql.functions.StringInspector

@RunWith(JUnitPlatform::class)
object AstSpec: Spek({

    describe("Basics") {

        it("describe") {
            testExpression("describe json 'test_data/nested.json';", """
                arrayval | Array<String>
                rownum | Number
                structval | Struct<{inner_key=String}>
            """.trimIndent())
        }

        it("simple select") {
            testExpression("select rownum, arrayval, structval from json 'test_data/nested.json' limit 3;", """
                1.0 | ["a1","a2","a3"] | {"inner_key":"a"}
                2.0 | ["b1","b2","b3"] | {"inner_key":"b"}
                3.0 | ["c1","c2","c3"] | {"inner_key":"c"}
            """.trimIndent())
        }

        it("select __all__") {
            testExpression("select __all__ from json 'test_data/nested.json' limit 3;", """
                {"rownum":1.0,"arrayval":["a1","a2","a3"],"structval":{"inner_key":"a"}}
                {"rownum":2.0,"arrayval":["b1","b2","b3"],"structval":{"inner_key":"b"}}
                {"rownum":3.0,"arrayval":["c1","c2","c3"],"structval":{"inner_key":"c"}}
            """.trimIndent())
        }

        it("select .nested") {
            testExpression("select rownum, structval.inner_key from json 'test_data/nested.json';", """
                1.0 | a
                2.0 | b
                3.0 | c
                4.0 | d
                5.0 | e
            """.trimIndent())
        }

        it("select .nested.nested") {
            testExpression("select rownum, structval.a.b from json 'test_data/deeply_nested.json';", """
                1.0 | a
                2.0 | b
                3.0 | c
                4.0 | d
                5.0 | e
            """.trimIndent())
        }

        it("lateral view") {
            testExpression("select rownum, arrayval from json 'test_data/nested.json' lateral view arrayval limit 5;", """
                1.0 | a1
                1.0 | a2
                1.0 | a3
                2.0 | b1
                2.0 | b2
            """.trimIndent())
        }

        it("[] function") {
            testExpression("select rownum, arrayval[1], arrayval['1'], structval['inner_key'] from json 'test_data/nested.json' limit 3;", """
                1.0 | a2 | a2 | a
                2.0 | b2 | b2 | b
                3.0 | c2 | c2 | c
            """.trimIndent())
        }

        it("[][] function") {
            testExpression("select rownum, arrayval[0][1], structval['a']['b'] from json 'test_data/deeply_nested.json' limit 3;", """
                1.0 | a1b | a
                2.0 | b1b | b
                3.0 | c1b | c
            """.trimIndent())
        }

        it("index dot on function") {
            testExpression("select max(structval).inner_key from json 'test_data/nested.json';", """
                e
            """.trimIndent())
        }
    }
})

private fun testExpression(expr: String, expected: String) {
    val results = mutableListOf<List<Any?>>()
    val operator = execute(expr)
    while (true) {
        operator.next()?.let { results.add(it) } ?: break
    }
    operator.close()

    assertThat(results.map { it.map { StringInspector.inspect(it) }.joinToString(" | ") }.joinToString("\n"), equalTo(expected.trim()))
}