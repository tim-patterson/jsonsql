package jsonsql.ast

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

import jsonsql.ast.Ast.*
import jsonsql.ast.Ast.Statement.*
import jsonsql.ast.Ast.Expression.*

@RunWith(JUnitPlatform::class)
object AstSpec: Spek({

    describe("astParser") {
        it("simple select") {
            assertThat(parse("select 1 from json 'dummy';"), equalTo(
                    Select(
                            listOf(NamedExpr(Constant(1.0), null)),
                            Source.Table(Table(TableType.JSON,"dummy"), null)
                    ) as Statement
            ))
        }

        it("bed maths") {
            testExpression("1 + 2 * 3 * (4 + 5)",
                    Function("add", listOf(
                            Constant(1.0),
                            Function("multiply", listOf(
                                    Function("multiply", listOf(
                                            Constant(2.0),
                                            Constant(3.0)
                                    )),
                                    Function("add", listOf(
                                            Constant(4.0),
                                            Constant(5.0)
                                    ))
                            ))
                    ))
            )
        }

        it("lower cases identifiers") {
            testExpression("ABC", Identifier(Field(null, "abc")))
        }
    }




})

private fun testExpression(expr: String, expected: Expression) {
    val actual = (parse("select $expr from json 'dummy';") as Select).expressions.first().expression
    assertThat(actual, equalTo(expected))
}