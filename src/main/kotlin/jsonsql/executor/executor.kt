package jsonsql.executor

import jsonsql.ast.parse
import jsonsql.logical.logicalOperatorTree
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.PhysicalTree
import jsonsql.physical.physicalOperatorTree

fun execute(query: String): PhysicalTree {
    val ast = parse(query)
    val logicalTree = logicalOperatorTree(ast)
    return physicalOperatorTree(logicalTree)
}
