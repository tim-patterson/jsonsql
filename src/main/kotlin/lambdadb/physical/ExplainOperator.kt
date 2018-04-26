package lambdadb.physical

class ExplainOperator(val stmt: Operator): Operator() {
    private lateinit var plan: Iterator<String>

    override fun columnAliases() = listOf("plan")

    override fun compile() {
        stmt.compile()
        plan = buildOutput(stmt).iterator()
    }

    override fun next(): List<Any?>? {
        return if (plan.hasNext()) {
            listOf(plan.next())
        } else {
            null
        }
    }

    override fun close() {} // Noop

    private fun buildOutput(operator: Operator,
                            outputLines: MutableList<String> = mutableListOf(),
                            indent: Int = 0): MutableList<String> {
        outputLines.add( (0 until indent).map { "  " }.joinToString("") + "$operator")
        operator.children().forEach { buildOutput(it, outputLines, indent + 1) }
        return outputLines
    }
}