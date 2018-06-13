package de.up.hpi.informationsystems.sampleapp

sealed class ApplicationException(message: String) extends Exception(message) {
  def this(message: String, cause: Throwable) = {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) = this(cause.toString, cause)

  def this() = this(null: String)
}

/** Indicates a failed authentication attempt with an authentication authority.
  *
  * @param message gives details
  */
case class AuthenticationFailedException(message: String) extends ApplicationException(message)

