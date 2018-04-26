package lambdadb.executor

import lambdadb.ast.parse
import lambdadb.logical.logicalOperatorTree
import lambdadb.physical.PhysicalOperator
import lambdadb.physical.physicalOperatorTree

fun execute(query: String): PhysicalOperator {
    val ast = parse(query)
    val logicalTree = logicalOperatorTree(ast)
    return physicalOperatorTree(logicalTree)
}
