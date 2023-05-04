package com.tutorial.custom.datasource

import com.tutorial.custom.datasource.engine.{DbColumn, DbConnectorProperties, DbQuery, DriverProvider}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.Connection
import java.util
import scala.collection.JavaConverters.setAsJavaSetConverter

class DbTable(
               providedQuery: String,
               providedSchema: Option[StructType],
               providedNumResultPartitions: Option[Long],
               dbConnectorProperties: DbConnectorProperties
             ) extends Table with SupportsRead with DriverProvider with Logging {

  private lazy val dataSchema: StructType = providedSchema match {
    case Some(schema) => schema
    case None => {
      obtainQuerySchemaFromTable()
    }
  }
  override def name(): String = { "CustomReaderTable" }

  override def schema(): StructType = {
    println("!===================================================! DbTable schema called")
    dataSchema
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    println("!===================================================! DbTable newScanBuilder called")
    val dbQuery = DbQuery(providedQuery = providedQuery, dataSchema = dataSchema)
    new DataScanBuilder(
      dbQuery = dbQuery,
      dbConnectorProperties = dbConnectorProperties,
      providedNumResultPartitions = providedNumResultPartitions
    )
  }

  private def obtainQuerySchemaFromTable(): StructType = {
    var connection: Connection = null
    try {
      val driver = getConfiguredDriver(dbConnectorProperties = dbConnectorProperties)

      connection = driver.connect(
        dbConnectorProperties.getConnectionUrl,
        dbConnectorProperties.getConnectionProperties
      )

      val oneRowQuery = createOneRowQuery(providedQuery)

      println(oneRowQuery)

      val statement = connection.createStatement()
      val res = statement.executeQuery(oneRowQuery)

      val resultMetadata = res.getMetaData

      println("column count")
      println(resultMetadata.getColumnCount)

      val dbColumns = Range(1, resultMetadata.getColumnCount + 1).map(num => {
        DbColumn(
          name = resultMetadata.getColumnLabel(num),
          number = num,
          dataType = resultMetadata.getColumnClassName(num),
          isNullable = resultMetadata.isNullable(num) == 1
        )
      })

      createSchemaFromDbColumns(dbColumns)
    } catch {
      case e: Exception => throw e
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  private def createSchemaFromDbColumns(dbColumns: Iterable[DbColumn]): StructType = {
    val schemaColumns = dbColumns.map(_.sparkColumn).toArray
    val resSchema = StructType(schemaColumns)

    println(resSchema)
    resSchema
  }

  private def createOneRowQuery(query: String): String = {
    s"""select *
       |from (
       |  ${query}
       |) t
       |limit 1 """.stripMargin
  }
}
