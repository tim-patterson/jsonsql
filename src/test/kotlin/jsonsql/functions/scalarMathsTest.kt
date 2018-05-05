package jsonsql.functions

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith



@RunWith(JUnitPlatform::class)
object ScalarMathSpec: Spek({
    describe("scalar maths") {
        action("add") {
            it("adds two numbers") {
                assertThat(AddFunction.execute(1.0, 2.0), equalTo(3.0))
            }

            it("adds a null") {
                assertThat(AddFunction.execute(1.0, null), nullValue())
            }
        }

        action("=") {
            it("equals") {
                assertThat(EqFunction.execute("abc", "abc"), equalTo(true))
            }

            it("not equals") {
                assertThat(EqFunction.execute("abc", "abcd"), equalTo(false))
            }

            it("with null") {
                assertThat(EqFunction.execute("abc", null), nullValue())
            }

            it("with double null") {
                assertThat(EqFunction.execute(null, null), nullValue())
            }
        }

        action("!=") {
            it("equals") {
                assertThat(NEqFunction.execute("abc", "abc"), equalTo(false))
            }

            it("not equals") {
                assertThat(NEqFunction.execute("abc", "abcd"), equalTo(true))
            }

            it("with null") {
                assertThat(NEqFunction.execute("abc", null), nullValue())
            }

            it("with double null") {
                assertThat(NEqFunction.execute(null, null), nullValue())
            }
        }
    }
})
