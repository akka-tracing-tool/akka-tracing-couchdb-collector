package pl.edu.agh.iet.akka_tracing.couchdb

import java.nio.charset.Charset

import org.asynchttpclient.AsyncHttpClient
import org.json4s.Extraction._
import org.json4s._
import org.json4s.native.JsonMethods._
import pl.edu.agh.iet.akka_tracing.couchdb.model.Document

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class CouchDatabase private[couchdb](baseUrl: String,
                                     name: String,
                                     client: AsyncHttpClient,
                                     user: Option[String],
                                     password: Option[String])
                                    (implicit ec: ExecutionContext) {

  import HttpClientUtils._

  private val databaseUrl = s"$baseUrl/$name"
  private implicit val formats = DefaultFormats

  def getAllDocs[T <: Document : Manifest]: Future[List[T]] = {
    val url = s"$databaseUrl/_all_docs"
    val request = buildRequest(url, "GET", user, password,
      queryParams = Some(List(
        "include_docs" -> "true"
      ))
    )
    client.executeRequest(request).toCompletableFuture.asScala flatMap { response =>
      Try(parse(response.getResponseBody(Charset.forName("utf-8")))) match {
        case Failure(ex) => Future.failed(ex)
        case Success(json) =>
          Future.successful(
            (json \ "rows").asInstanceOf[JArray].arr.map(doc => extract[T](doc))
          )
      }
    }
  }

  def getDocs[T <: Document : Manifest](ids: List[String]): Future[List[T]] = {
    val url = s"$databaseUrl/_all_docs"
    val request = buildRequest(url, "POST", user, password,
      queryParams = Some(List(
        "include_docs" -> "true"
      )),
      body = Some(compact(render(JObject(
        "keys" -> JArray(
          ids.map(id => JString(id))
        )
      ))))
    )
    client.executeRequest(request).toCompletableFuture.asScala flatMap { response =>
      Try(parse(response.getResponseBody(Charset.forName("utf-8")))) match {
        case Failure(ex) => Future.failed(ex)
        case Success(json) =>
          Future.successful(
            (json \ "rows").asInstanceOf[JArray].arr.map(doc => extract[T](doc))
          )
      }
    }
  }

  def putDocs[T <: Document](docs: List[T]): Future[Unit] = {
    val url = s"$databaseUrl/_bulk_docs"
    val request = buildRequest(url, "POST", user, password,
      body = Some(compact(render(JObject(
        "docs" -> JArray(
          docs.map(doc => decompose(doc))
        )
      ))))
    )
    client.executeRequest(request).toCompletableFuture.asScala.map(_ => ())
  }

  def attemptToCreate: Future[Unit] = {
    val request = buildRequest(databaseUrl, "PUT", user, password)
    client.executeRequest(request).toCompletableFuture.asScala.map(_ => ())
      .recover({ case _ => () })
  }

  def deleteAllDocs(): Future[Unit] = {
    val getUrl = s"$databaseUrl/_all_docs"
    val getRequest = buildRequest(getUrl, "GET", user, password)
    client.executeRequest(getRequest).toCompletableFuture.asScala flatMap { response =>
      Try(parse(response.getResponseBody(Charset.forName("utf-8")))) match {
        case Failure(ex) => Future.failed(ex)
        case Success(json) =>
          val docs = (json \ "rows").asInstanceOf[JArray].arr
          val deleteUrl = s"$databaseUrl/_bulk_docs"
          val deleteRequest = buildRequest(deleteUrl, "POST", user, password,
            body = Some(compact(render(JObject(
              "docs" -> JArray(
                docs map { doc =>
                  JObject(
                    "_id" -> doc \ "id",
                    "_rev" -> doc \ "value" \ "rev",
                    "_deleted" -> JBool(true)
                  )
                }
              )
            ))))
          )
          client.executeRequest(deleteRequest).toCompletableFuture.asScala.map { _ => () }
      }
    }
  }
}
