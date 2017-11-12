package pl.edu.agh.iet.akka_tracing.couchdb

import java.nio.charset.{ Charset, StandardCharsets }

import org.asynchttpclient.AsyncHttpClient
import org.json4s.Extraction._
import org.json4s._
import org.json4s.ext.JavaTypesSerializers
import org.json4s.native.JsonMethods._
import pl.edu.agh.iet.akka_tracing.couchdb.model.Document

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class CouchDatabase private[couchdb](
    baseUrl: String,
    name: String,
    client: AsyncHttpClient,
    user: Option[String],
    password: Option[String]
)(
    implicit ec: ExecutionContext
) {

  import CouchDbUtils._

  private implicit val formats: Formats = DefaultFormats ++ JavaTypesSerializers.all
  private val DatabaseUrl: String = s"$baseUrl/$name"
  private val Utf8Charset: Charset = StandardCharsets.UTF_8

  def getAllDocs[T <: Document : Manifest]: Future[List[T]] = {
    val url = s"$DatabaseUrl/_all_docs"
    val request = buildRequest(url, "GET", user, password,
      queryParams = Some(List(
        "include_docs" -> "true"
      ))
    )
    client.executeRequest(request).toCompletableFuture.asScala flatMap { response =>
      Try(parse(response.getResponseBody(Utf8Charset))) match {
        case Failure(ex) => Future.failed(ex)
        case Success(json) =>
          json \ "rows" match {
            case JArray(rows) => Future.successful(rows.map(row => extract[T](row \ "doc")))
            case _ => Future.successful(List())
          }
      }
    }
  }

  def getDocs[T <: Document : Manifest](ids: List[String]): Future[List[T]] = {
    val url = s"$DatabaseUrl/_all_docs"
    val request = buildRequest(url, "POST", user, password,
      queryParams = Some(List(
        "include_docs" -> "true"
      )),
      body = Some(JObject(
        "keys" -> JArray(
          ids.map(id => JString(id))
        )
      ))
    )
    client.executeRequest(request).toCompletableFuture.asScala flatMap { response =>
      Try(parse(response.getResponseBody(Utf8Charset))) match {
        case Failure(ex) => Future.failed(ex)
        case Success(json) =>
          json \ "rows" match {
            case JArray(rows) =>
              Future.successful(rows
                .filter(row => (row \ "doc").isDefined)
                .map(row => extract[T](row \ "doc")))
            case _ => Future.successful(List())
          }
      }
    }
  }

  def putDoc[T <: Document](doc: T): Future[Unit] = {
    val url = s"$DatabaseUrl/${ doc._id }"
    val request = buildRequest(url, "PUT", user, password,
      body = Some(decompose(doc))
    )
    client.executeRequest(request).toCompletableFuture.asScala.map(_ => ())
  }

  def putDocs[T <: Document](docs: List[T]): Future[Unit] = {
    val url = s"$DatabaseUrl/_bulk_docs"
    val request = buildRequest(url, "POST", user, password,
      body = Some(JObject(
        "docs" -> JArray(
          docs.map(doc => decompose(doc))
        )
      ))
    )
    client.executeRequest(request).toCompletableFuture.asScala.map(_ => ())
  }

  def attemptToCreate: Future[Unit] = {
    val request = buildRequest(DatabaseUrl, "PUT", user, password)
    client.executeRequest(request).toCompletableFuture.asScala.map(_ => ())
      .recover({ case _ => () })
  }

  def deleteAllDocs(): Future[Unit] = {
    val getUrl = s"$DatabaseUrl/_all_docs"
    val getRequest = buildRequest(getUrl, "GET", user, password)
    client.executeRequest(getRequest).toCompletableFuture.asScala flatMap { response =>
      Try(parse(response.getResponseBody(Utf8Charset))) match {
        case Failure(ex) => Future.failed(ex)
        case Success(json) =>
          json \ "rows" match {
            case JArray(docs) =>
              val deleteUrl = s"$DatabaseUrl/_bulk_docs"
              val deleteRequest = buildRequest(deleteUrl, "POST", user, password,
                body = Some(JObject(
                  "docs" -> JArray(
                    docs map { doc =>
                      JObject(
                        "_id" -> doc \ "id",
                        "_rev" -> doc \ "value" \ "rev",
                        "_deleted" -> JBool(true)
                      )
                    }
                  )
                ))
              )
              client.executeRequest(deleteRequest).toCompletableFuture.asScala.map { _ => () }
            case _ => Future.successful(())
          }
      }
    }
  }

  def compactDb(): Future[Unit] = {
    val request = buildRequest(s"$DatabaseUrl/_compact", "POST", user, password)
    client.executeRequest(request).toCompletableFuture.asScala map { _ => () }
  }
}
