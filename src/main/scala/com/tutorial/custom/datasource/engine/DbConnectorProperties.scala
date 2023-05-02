package com.tutorial.custom.datasource.engine

import java.util.Properties

case class DbConnectorProperties(
                                  driverClassName: String,
                                  host: String,
                                  port: Option[String],
                                  user: String, password: String,
                                  schema: Option[String]
                                ) {
  def getConnectionProperties: Properties = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    props
  }

  def getConnectionUrl: String = {
    val connectionUrlBase = s"jdbc:postgresql://${host}"
    var connectionUrl = port match {
      case Some(portName) => connectionUrlBase.concat(s":${portName}")
      case None => connectionUrlBase
    }

    connectionUrl = schema match {
      case Some(schemaName) => connectionUrl.concat(s"/${schemaName}")
      case None => connectionUrl.concat("/")
    }

    connectionUrl
  }

  override def toString: String = {
    s"""Provided DB Connector properties:
       | Driver: ${driverClassName}
       | host: ${host}
       | port: ${port}
       | schema: ${schema}""".stripMargin
  }
}
