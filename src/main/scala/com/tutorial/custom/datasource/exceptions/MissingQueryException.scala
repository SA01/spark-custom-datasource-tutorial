package com.tutorial.custom.datasource.exceptions

class MissingQueryException(message: String) extends Exception {
  override def getMessage: String = message
}
