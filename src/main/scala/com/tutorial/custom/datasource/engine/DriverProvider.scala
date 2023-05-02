package com.tutorial.custom.datasource.engine

import java.sql.{Driver, DriverManager}
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter

trait DriverProvider {
  def getConfiguredDriver(dbConnectorProperties: DbConnectorProperties): Driver = {
    DriverManager
      .getDrivers
      .asScala
      .filter(_.getClass.getName == dbConnectorProperties.driverClassName)
      .next()
  }
}
