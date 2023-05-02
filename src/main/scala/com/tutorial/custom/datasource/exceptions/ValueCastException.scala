package com.tutorial.custom.datasource.exceptions

class ValueCastException(message: String) extends Exception {
  override def getMessage: String = message
}
