package pl.edu.agh.iet.akka_tracing.couchdb.model

trait Document {
  def _id: String
  def _rev: Option[String]
}
