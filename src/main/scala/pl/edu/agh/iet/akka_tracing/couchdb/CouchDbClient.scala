package pl.edu.agh.iet.akka_tracing.couchdb

import com.typesafe.config.Config
import org.asynchttpclient.DefaultAsyncHttpClient
import pl.edu.agh.iet.akka_tracing.config.ConfigUtils

import scala.concurrent.ExecutionContext

class CouchDbClient(host: String,
                    port: Int,
                    useHttps: Boolean,
                    user: Option[String],
                    password: Option[String])
                   (implicit ec: ExecutionContext) {

  private[couchdb] val asyncHttpClient = new DefaultAsyncHttpClient()

  private val protocol =
    if (useHttps) {
      "https"
    } else {
      "http"
    }

  def getDb(name: String): CouchDatabase =
    new CouchDatabase(s"$protocol://$host:$port", name, asyncHttpClient, user, password)

}

object CouchDbClient {

  import ConfigUtils._

  def apply(config: Config)(implicit ec: ExecutionContext): CouchDbClient = {
    val host = config.getOrElse[String]("host", "localhost")
    val useHttps = config.getOrElse[Boolean]("useHttps", true)
    val port = config.getOrElse[Int]("port",
      if (useHttps) {
        6984
      } else {
        5984
      }
    )
    val user = config.getOption[String]("user")
    val password = config.getOption[String]("password")
    new CouchDbClient(host, port, useHttps, user, password)
  }
}
