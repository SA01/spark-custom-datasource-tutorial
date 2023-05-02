package com.tutorial.custom.datasource.engine

import org.apache.spark.sql.connector.read.InputPartition

case class DataPartition(
                          dbQuery: DbQuery,
                          startRow: Long,
                          endRow: Long,
                          pushedFilters: Iterable[PushedFilter]
                        ) extends InputPartition {
  lazy val queryWithAppliedConstraints: String = {
    if (pushedFilters.nonEmpty) {
      val filtersString = pushedFilters.map(_.filterSqlClause).mkString("\n AND ")
      s"""${dbQuery.dataReadQuery}
         | WHERE ${filtersString}
         | LIMIT ${(endRow - startRow)} OFFSET ${startRow} """.stripMargin
    } else {
      s"""${dbQuery.dataReadQuery}
         | LIMIT ${(endRow - startRow)} OFFSET ${startRow} """.stripMargin
    }
  }

  override def toString: String = {
    f"""Query: ${dbQuery.dataReadQuery}
       |startRow: ${startRow}
       |endRow: ${endRow} """.stripMargin
  }
}
