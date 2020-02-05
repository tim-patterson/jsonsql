package jsonsql.integration

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe


@RunWith(JUnitPlatform::class)
object HttpTest: Spek({

    describe("Http Reads") {
        it("works", timeout=30000) {
            testQuery("select count() from (select children.data from json 'https://www.reddit.com/r/all.json?limit=100' lateral view data.children);", """
                100.0
            """.trimIndent())
        }
    }
}) {
}