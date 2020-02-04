package jsonsql.integration

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe


@RunWith(JUnitPlatform::class)
object S3Test: Spek({

    describe("S3 Reads") {
        it("works", timeout=30000) {
            testQuery("describe json 's3://ap-southeast-2.elasticmapreduce/samples/hive-ads/tables/impressions/';", """
                adid | String
                browsercookie | String
                hostname | String
                impressionid | String
                ip | String
                modelid | String
                number | String
                processid | String
                referrer | String
                requestbegintime | String
                requestendtime | String
                sessionid | String
                threadid | String
                timers | Struct<{modelLookup=String, requestTime=String}>
                useragent | String
                usercookie | String
            """.trimIndent())
        }
    }
}) {
}