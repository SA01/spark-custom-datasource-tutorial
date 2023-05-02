package com.tutorial.custom.datasource.exceptions

class MissingConnectionPropertiesException(message: String) extends Exception {
  override def getMessage: String = message
}
