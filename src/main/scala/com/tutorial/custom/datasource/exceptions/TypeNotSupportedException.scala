package com.tutorial.custom.datasource.exceptions

class TypeNotSupportedException(message: String) extends Exception {
  override def getMessage: String = message
}
