package pl.edu.agh.iet.akka_tracing.couchdb.model

import org.json4s._
import pl.edu.agh.iet.akka_tracing.couchdb.CouchDbUtils

case class ReplicationRequest(
    source: ReplicationDbConfig,
    target: ReplicationDbConfig,
    continuous: Boolean = true,
    createDb: Boolean = false
)

sealed trait ReplicationDbConfig {
  def toJSON: JValue
}

case class RemoteReplicationDbConfig(
    host: String,
    port: Int,
    database: String,
    useHttps: Boolean = true,
    user: Option[String] = None,
    password: Option[String] = None
) extends ReplicationDbConfig {

  import CouchDbUtils._

  override def toJSON: JValue = {
    val protocol =
      if (useHttps) {
        "https"
      } else {
        "http"
      }
    val url = s"$protocol://$host:$port/$database"
    (user, password) match {
      case (Some(userValue), Some(passwordValue)) =>
        JObject(
          "url" -> JString(url),
          "headers" -> JObject(
            "Authorization" -> JString(s"Basic ${ getBase64AuthValue(userValue, passwordValue) }")
          )
        )
      case _ => JString(url)
    }
  }
}

case class LocalReplicationDbConfig(db: String) extends ReplicationDbConfig {
  override def toJSON: JValue = JString(db)
}
