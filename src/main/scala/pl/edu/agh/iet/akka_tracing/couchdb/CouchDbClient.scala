package pl.edu.agh.iet.akka_tracing.couchdb

import com.typesafe.config.Config
import org.asynchttpclient.{ DefaultAsyncHttpClient, DefaultAsyncHttpClientConfig }
import org.json4s._
import org.json4s.ext.JavaTypesSerializers
import pl.edu.agh.iet.akka_tracing.config.ConfigUtils
import pl.edu.agh.iet.akka_tracing.couchdb.model.ReplicationRequest

import scala.concurrent.{ ExecutionContext, Future }

class CouchDbClient(
    host: String,
    port: Int,
    useHttps: Boolean,
    user: Option[String],
    password: Option[String],
    connectionTimeout: Int,
    requestTimeout: Int
)(
    implicit ec: ExecutionContext
) {

  import CouchDbUtils._

  private[couchdb] val asyncHttpClient = new DefaultAsyncHttpClient(
    new DefaultAsyncHttpClientConfig.Builder()
      .setConnectTimeout(connectionTimeout)
      .setRequestTimeout(requestTimeout)
      .build()
  )
  private implicit val formats: Formats = DefaultFormats ++ JavaTypesSerializers.all

  private val protocol =
    if (useHttps) {
      "https"
    } else {
      "http"
    }

  private[couchdb] val baseUrl = s"$protocol://$host:$port"

  def getDb(name: String): CouchDatabase =
    new CouchDatabase(baseUrl, name, asyncHttpClient, user, password)

  def replicate(replicationRequest: ReplicationRequest): Future[Unit] = {
    val bodyBase = JObject(
      "source" -> replicationRequest.source.toJSON,
      "target" -> replicationRequest.target.toJSON,
      "continuous" -> JBool(replicationRequest.continuous)
    )
    val body =
      if (replicationRequest.createDb) {
        bodyBase merge JObject("create_target" -> JBool(true))
      } else {
        bodyBase
      }
    val request = buildRequest(s"$baseUrl/_replicate", "POST", user, password,
      body = Some(body)
    )
    asyncHttpClient.executeRequest(request).toCompletableFuture.asScala.map(
      _ => ()
    )
  }

  def close(): Unit = asyncHttpClient.close()

}

object CouchDbClient {

  import ConfigUtils._

  def apply(databaseConfig: Config, httpConfig: Config)
    (implicit ec: ExecutionContext): CouchDbClient = {
    val host = databaseConfig.getOrElse[String]("host", "localhost")
    val useHttps = databaseConfig.getOrElse[Boolean]("useHttps", true)
    val port = databaseConfig.getOrElse[Int]("port",
      if (useHttps) {
        6984
      } else {
        5984
      }
    )
    val user = databaseConfig.getOption[String]("user")
    val password = databaseConfig.getOption[String]("password")
    val connectionTimeout = httpConfig.getOption[Int]("connectionTimeout").getOrElse(2000)
    val requestTimeout = httpConfig.getOption[Int]("requestTimeout").getOrElse(10000)
    new CouchDbClient(host, port, useHttps, user, password, connectionTimeout, requestTimeout)
  }
}
