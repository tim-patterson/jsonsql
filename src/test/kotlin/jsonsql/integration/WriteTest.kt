package jsonsql.integration

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import java.io.File

import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe


@RunWith(JUnitPlatform::class)
object WriteTest: Spek({

    describe("Read Write") {

        it("csv") {
            File("test_data/out").deleteRecursively()
            testQuery("select count() from csv 'test_data/out/test.csv';", """
                0.0
            """.trimIndent())

            testQuery("insert into csv 'test_data/out/test.csv' select rownum, arrayval[0] as foo from json 'test_data/nested.json';", """
                5 rows written to "test_data/out/test.csv"
            """.trimIndent())

            testQuery("select rownum, foo from csv 'test_data/out/test.csv';", """
                1.0 | a1
                2.0 | b1
                3.0 | c1
                4.0 | d1
                5.0 | e1
            """.trimIndent())
        }

        it("json") {
            File("test_data/out").deleteRecursively()
            testQuery("select count() from json 'test_data/out/test.json';", """
                0.0
            """.trimIndent())

            testQuery("insert into json 'test_data/out/test.json' select rownum, arrayval[0] as foo from json 'test_data/nested.json';", """
                5 rows written to "test_data/out/test.json"
            """.trimIndent())

            testQuery("select rownum, foo from json 'test_data/out/test.json';", """
                1.0 | a1
                2.0 | b1
                3.0 | c1
                4.0 | d1
                5.0 | e1
            """.trimIndent())
        }
    }
})