package jsonsql.integration

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

@RunWith(JUnitPlatform::class)
object JoinTest: Spek({
    describe("Joins") {
        it("basic") {
            testQuery("select a.rownum, b.rownum from json 'test_data/nested.json' as a join json 'test_data/nested.json' b on a.rownum + 1= b.rownum;", """
                1.0 | 2.0
                2.0 | 3.0
                3.0 | 4.0
                4.0 | 5.0
            """.trimIndent())
        }
    }
})