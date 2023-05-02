package com.tutorial.custom.datasource.engine

import org.apache.spark.sql.types.StructType

case class DbQuery(providedQuery: String, dataSchema: StructType) {
  lazy val dataReadQuery: String = {
      val fields = extractFieldsToSelect(schema = dataSchema)

      val resQuery =
        f"""select
           |${fields.mkString(",\n ")}
           |from (
           |  ${providedQuery}
           |) t """.stripMargin

      resQuery
  }

  lazy val numRowsQuery: String = {
    s"""select count(1) as num_rows
       |from (
       |  ${providedQuery}
       |) t """.stripMargin
  }

  private def extractFieldsToSelect(schema: StructType): Iterable[String] = {
    schema.fields.map(_.name)
  }
}
