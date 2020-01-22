package jsonsql.executor

import jsonsql.ast.parse
import jsonsql.logical.logicalOperatorTree
import jsonsql.physical.PhysicalTree
import jsonsql.physical.physicalOperatorTree
import org.apache.arrow.memory.RootAllocator

fun execute(query: String): PhysicalTree {
    val ast = parse(query)
    val logicalTree = logicalOperatorTree(ast)
    val allocator = RootAllocator()
    // Physical operator tree will clean up allocator on close
    return physicalOperatorTree(allocator, logicalTree)
}
