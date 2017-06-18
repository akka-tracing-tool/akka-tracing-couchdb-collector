package pl.edu.agh.iet.akka_tracing.couchdb

case class RequestFailedException(status: Int,
                                  message: String = s"Request resulted in error")
  extends RuntimeException
