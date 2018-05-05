package jsonsql.integration

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith


@RunWith(JUnitPlatform::class)
object BasicTest: Spek({

    describe("Basics") {

        it("describe") {
            testQuery("describe json 'test_data/nested.json';", """
                arrayval | Array<String>
                rownum | Number
                structval | Struct<{inner_key=String}>
            """.trimIndent())
        }

        it("describe csv") {
            testQuery("describe csv 'test_data/simple.csv';", """
                field1 | String
                field2 | String
                _col_2 | String
            """.trimIndent())
        }

        it("simple select") {
            testQuery("select rownum, arrayval, structval from json 'test_data/nested.json' limit 3;", """
                1.0 | ["a1","a2","a3"] | {"inner_key":"a"}
                2.0 | ["b1","b2","b3"] | {"inner_key":"b"}
                3.0 | ["c1","c2","c3"] | {"inner_key":"c"}
            """.trimIndent())
        }

        it("simple select csv") {
            testQuery("select field1, field2 from csv 'test_data/simple.csv';", """
                1 | quoted ,
                2 | a
                3 | foobar
            """.trimIndent())
        }

        it("select __all__") {
            testQuery("select __all__ from json 'test_data/nested.json' limit 3;", """
                {"rownum":1.0,"arrayval":["a1","a2","a3"],"structval":{"inner_key":"a"}}
                {"rownum":2.0,"arrayval":["b1","b2","b3"],"structval":{"inner_key":"b"}}
                {"rownum":3.0,"arrayval":["c1","c2","c3"],"structval":{"inner_key":"c"}}
            """.trimIndent())
        }

        it("select .nested") {
            testQuery("select rownum, structval.inner_key from json 'test_data/nested.json';", """
                1.0 | a
                2.0 | b
                3.0 | c
                4.0 | d
                5.0 | e
            """.trimIndent())
        }

        it("select .nested.nested") {
            testQuery("select rownum, structval.a.b from json 'test_data/deeply_nested.json';", """
                1.0 | a
                2.0 | b
                3.0 | c
                4.0 | d
                5.0 | e
            """.trimIndent())
        }

        it("lateral view") {
            testQuery("select rownum, arrayval from json 'test_data/nested.json' lateral view arrayval limit 5;", """
                1.0 | a1
                1.0 | a2
                1.0 | a3
                2.0 | b1
                2.0 | b2
            """.trimIndent())
        }

        it("[] function") {
            testQuery("select rownum, arrayval[1], arrayval['1'], arrayval[6], structval['inner_key'] from json 'test_data/nested.json' limit 3;", """
                1.0 | a2 | a2 | null | a
                2.0 | b2 | b2 | null | b
                3.0 | c2 | c2 | null | c
            """.trimIndent())
        }

        it("[][] function") {
            testQuery("select rownum, arrayval[0][1], structval['a']['b'] from json 'test_data/deeply_nested.json' limit 3;", """
                1.0 | a1b | a
                2.0 | b1b | b
                3.0 | c1b | c
            """.trimIndent())
        }

        it("index dot on function") {
            testQuery("select max(structval).inner_key from json 'test_data/nested.json';", """
                e
            """.trimIndent())
        }
    }
})