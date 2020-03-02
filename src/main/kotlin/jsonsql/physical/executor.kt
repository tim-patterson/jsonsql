package jsonsql.physical

import jsonsql.query.parse

fun operatorTreeFromSql(sql: String): PhysicalTree {
    val query = parse(sql)
    return physicalOperatorTree(query)
}
