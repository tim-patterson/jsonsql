package jsonsql.integration

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe


@RunWith(JUnitPlatform::class)
object TopLevelArrayTest: Spek({
    describe("Json formats") {
        it("raw values/ndjson") {
            testQuery("select sum(user_id) from json 'test_data/update_events.json';", """
                10.0
            """.trimIndent())
        }

        it("top level array") {
            testQuery("select sum(user_id) from json 'test_data/update_events_as_array.json';", """
                10.0
            """.trimIndent())
        }

        it("mixed bag") {
            testQuery("select sum(user_id) from json 'test_data/update_events_as_mixed.json';", """
                10.0
            """.trimIndent())
        }
    }

})