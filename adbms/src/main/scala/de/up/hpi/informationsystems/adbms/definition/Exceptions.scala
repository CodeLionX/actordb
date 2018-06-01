package de.up.hpi.informationsystems.adbms.definition


sealed class AdbmsException(message: String) extends Exception(message) {
  def this(message: String, cause: Throwable) = {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) = this(cause.toString, cause)

  def this() = this(null: String)
}
/**
  * Indicates that the supplied column definition is not applicable to the current schema.
  *
  * @param message gives details
  */
case class IncompatibleColumnDefinitionException(message: String) extends AdbmsException(message)

/**
  * Indicates that the supplied record could not be found.
  *
  * @param message gives details
  */
case class RecordNotFoundException(message: String) extends AdbmsException(message)

/**
  * Indicates that a Dactor or Relation was found in an inconsistent or unexpected state.
  *
  * @param message gives details
  */
case class InconsistentStateException(message: String) extends AdbmsException(message)
