package jsonsql.physical.operators

import jsonsql.query.Field
import jsonsql.physical.*

class ExplainOperator(
        private val stmt: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases = listOf(Field(null, "plan"))

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        return buildOutput(stmt).asSequence().map { listOf(it) }.withClose()
    }

    private fun buildOutput(operator: PhysicalOperator,
                            outputLines: MutableList<String> = mutableListOf(),
                            indent: Int = 0): MutableList<String> {
        outputLines.add( (0 until indent).map { "  " }.joinToString("") + "$operator")
        //TODO maybe we do need children? or we incorporate into an overridable toString
        //operator.children().forEach { buildOutput(it, outputLines, indent + 1) }
        return outputLines
    }
}