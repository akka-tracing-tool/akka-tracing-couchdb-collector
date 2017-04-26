package pl.edu.agh.iet.akka_tracing.utils

import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.LoggerFactory
import pl.edu.agh.iet.akka_tracing.config.ConfigUtils
import pl.edu.agh.iet.akka_tracing.couchdb.CouchDbClient

import scala.concurrent.{ ExecutionContext, Future }

class DatabaseUtils(config: Config)(implicit ec: ExecutionContext) {

  import ConfigUtils._

  private val dbConfig = config.getOrElse[Config]("database", ConfigFactory.empty("db"))
  private val logger = LoggerFactory.getLogger(getClass)

  private[akka_tracing] val couchDbClient = CouchDbClient(dbConfig)

  private[akka_tracing] val receiverMessagesDatabase = couchDbClient.getDb("receiver_messages")
  private[akka_tracing] val senderMessagesDatabase = couchDbClient.getDb("sender_messages")
  private[akka_tracing] val messagesRelationsDatabase = couchDbClient.getDb("messages_relations")

  def init: Future[Unit] = {
    logger.info("Creating databases (if necessary)...")
    Future.sequence(Seq(
      senderMessagesDatabase.attemptToCreate,
      receiverMessagesDatabase.attemptToCreate,
      messagesRelationsDatabase.attemptToCreate
    )) map { _ => logger.info("Done") }
  }

  def clean: Future[Unit] = {
    logger.info("Cleaning databases...")
    Future.sequence(Seq(
      senderMessagesDatabase.deleteAllDocs(),
      receiverMessagesDatabase.deleteAllDocs(),
      messagesRelationsDatabase.deleteAllDocs()
    )) map { _ => logger.info("Done") }
  }
}
