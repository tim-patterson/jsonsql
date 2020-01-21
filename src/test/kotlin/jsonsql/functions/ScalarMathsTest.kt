package jsonsql.functions

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe


@RunWith(JUnitPlatform::class)
object ScalarMathTest: Spek({
    describe("scalar maths") {
        describe("add") {
            it("adds two numbers") {
                assertThat(AddFunction.execute(1.0, 2.0), equalTo(3.0))
            }

            it("adds a null") {
                assertThat(AddFunction.execute(1.0, null), nullValue())
            }
        }

        describe("=") {
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

        describe("!=") {
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
