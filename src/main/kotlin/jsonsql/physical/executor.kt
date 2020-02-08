package jsonsql.physical

import jsonsql.query.parse
import jsonsql.logical.logicalOperatorTree

fun operatorTreeFromSql(query: String): PhysicalTree {
    val ast = parse(query)
    val logicalTree = logicalOperatorTree(ast)
    return physicalOperatorTree(logicalTree)
}
