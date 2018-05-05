package jsonsql.integration

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith


@RunWith(JUnitPlatform::class)
object AliasTest: Spek({
    describe("Column Aliases") {
        it("basic with as") {
            testQuery("select a from (select rownum as a from json 'test_data/nested.json' limit 3);", """
                1.0
                2.0
                3.0
            """.trimIndent())
        }

        it("basic without as") {
            testQuery("select a from (select rownum a from json 'test_data/nested.json' limit 3);", """
                1.0
                2.0
                3.0
            """.trimIndent())
        }
    }

    describe("Table Aliases") {
        it("basic") {
            testQuery("select my_tbl.rownum, rownum from json 'test_data/nested.json' as my_tbl where my_tbl.rownum > 1 limit 3;", """
                2.0 | 2.0
                3.0 | 3.0
                4.0 | 4.0
            """.trimIndent())
        }

        it("lateral view unqualified") {
            testQuery("select foo.rownum, arrayval from json 'test_data/nested.json' foo lateral view arrayval limit 5;", """
                1.0 | a1
                1.0 | a2
                1.0 | a3
                2.0 | b1
                2.0 | b2
            """.trimIndent())
        }

        it("lateral view qualified") {
            testQuery("select foo.rownum, arrayval from json 'test_data/nested.json' foo lateral view foo.arrayval order by rownum, arrayval limit 5;", """
                1.0 | a1
                1.0 | a2
                1.0 | a3
                2.0 | b1
                2.0 | b2
            """.trimIndent())
        }
    }
})