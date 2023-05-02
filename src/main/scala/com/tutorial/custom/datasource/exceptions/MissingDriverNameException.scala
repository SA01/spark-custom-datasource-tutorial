package com.tutorial.custom.datasource.exceptions

class MissingDriverNameException(message: String) extends Exception {
  override def getMessage: String = message
}
