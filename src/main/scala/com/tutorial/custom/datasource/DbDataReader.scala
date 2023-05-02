package com.tutorial.custom.datasource

import com.tutorial.custom.datasource.engine.DbConnectorProperties
import com.tutorial.custom.datasource.exceptions.{
  MissingConnectionPropertiesException, MissingDriverNameException, MissingQueryException
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

// TODO: Create driver not present error

class DbDataReader extends TableProvider with Logging {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // TODO: For demonstration
    println("!===================================================! DbDataReader: inferSchema called")
    println(options.asCaseSensitiveMap())

    null
  }

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = {
    // TODO: For demonstration
    println("!===================================================! DbDataReader getTable called")
    println(schema)
    println(partitioning.mkString("Array(", ", ", ")"))

    println("properties")
    println(properties)
    println("!===================================================!")

    if (!properties.containsKey("path")) {
      throw new MissingQueryException("No table or query provided")
    }

    val queryString = properties.get("path")
    val numPartitions = if (properties.get("partitions") == null) {
      None
    } else {
      Some(properties.get("partitions").toLong)
    }

    val dbConnectorProperties = extractDbConnectionProperties(povidedProperties = properties)
    log.info(s"Provided schema: ${if(schema != null) schema.mkString("\n") else null}")

    new DbTable(
      providedQuery = queryString,
      providedSchema = Option(schema),
      providedNumResultPartitions = numPartitions,
      dbConnectorProperties = dbConnectorProperties
    )
  }

  override def supportsExternalMetadata(): Boolean = true

  private def extractDbConnectionProperties(povidedProperties: util.Map[String, String]): DbConnectorProperties = {
    if (!povidedProperties.containsKey("DriverClass")) {
      throw new MissingDriverNameException(
        "No driver class name set. A JDBC compliant driver class name should be provided."
      )
    }

    if (
      !povidedProperties.containsKey("host") ||
        !povidedProperties.containsKey("user") ||
        !povidedProperties.containsKey("password")
    ) {
      throw new MissingConnectionPropertiesException(
        "Host (host), username (user) and password (password) must be provided"
      )
    }

    val dbConnectorProperties = DbConnectorProperties(
      driverClassName = povidedProperties.get("DriverClass"),
      host = povidedProperties.get("host"),
      port = if (povidedProperties.containsKey("port")) Some(povidedProperties.get("port")) else None,
      user = povidedProperties.get("user"),
      password = povidedProperties.get("password"),
      schema = if (povidedProperties.containsKey("schema")) Some(povidedProperties.get("schema")) else None
    )

    log.info(dbConnectorProperties.toString)

    dbConnectorProperties
  }
}
