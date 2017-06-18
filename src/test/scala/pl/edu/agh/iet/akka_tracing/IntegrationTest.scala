package pl.edu.agh.iet.akka_tracing

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.json4s._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FlatSpec, Matchers }
import pl.edu.agh.iet.akka_tracing.collector.CouchDbCollector
import pl.edu.agh.iet.akka_tracing.model.{ Message, MessagesRelation, ReceiverMessage, SenderMessage }
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils
import pl.edu.agh.iet.akka_tracing.visualization.data.CouchDbDataSource

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * This test requires docker environment created by running travis_before.sh script.
  */
class IntegrationTest extends FlatSpec with Matchers with ScalaFutures {
  private val masterConfig = ConfigFactory.load("master.conf")
  private val slaveConfig = ConfigFactory.load("slave.conf")
  private val uuid1: UUID = UUID.randomUUID()
  private val uuid2: UUID = UUID.randomUUID()

  "CouchDB collector and data source" should "persist messages and perform replication" in {
    val masterDatabaseUtils = new DatabaseUtils(masterConfig)
    val slaveDatabaseUtils = new DatabaseUtils(slaveConfig)

    Await.result(masterDatabaseUtils.init, Duration.Inf)
    Await.result(masterDatabaseUtils.clean, Duration.Inf)

    Await.result(slaveDatabaseUtils.init, Duration.Inf)
    Await.result(slaveDatabaseUtils.clean, Duration.Inf)

    val couchDbCollector = new CouchDbCollector(masterConfig)

    couchDbCollector.handleSenderMessage(
      SenderMessage(uuid1, "sender1", Some(JObject("test" -> JBool(true))))
    )
    couchDbCollector.handleSenderMessage(
      SenderMessage(uuid2, "sender2", None)
    )
    couchDbCollector.handleReceiverMessage(ReceiverMessage(uuid1, "receiver1"))
    couchDbCollector.handleRelationMessage(MessagesRelation(uuid1, uuid2))

    // Wait for operations on DBs and replication
    Thread.sleep(5000)

    val couchDbDataSource = new CouchDbDataSource(slaveConfig)

    val messagesFuture = couchDbDataSource.getMessages
    val relationsFuture = couchDbDataSource.getRelations
    val future = for {
      messages <- messagesFuture
      relations <- relationsFuture
    } yield (messages, relations)

    whenReady(future) {
      case (messages, relations) =>
        messages should contain(Message(uuid1, "sender1", Some("receiver1"), Some(JObject("test" -> JBool(true)))))
        messages should contain(Message(uuid2, "sender2", None, None))
        messages should have length 2

        relations should contain(MessagesRelation(uuid1, uuid2))
        relations should have length 1
    }
  }
}
