package pl.edu.agh.iet.akka_tracing

import java.util.UUID

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.json4s._
import org.scalatest.{ FlatSpec, Matchers }
import org.scalatest.concurrent.ScalaFutures
import pl.edu.agh.iet.akka_tracing.collector.CouchDbCollectorConstructor
import pl.edu.agh.iet.akka_tracing.model._
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils
import pl.edu.agh.iet.akka_tracing.visualization.data.CouchDbDataSource

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * This test requires docker environment created by running travis_before.sh script.
  */
class IntegrationTest extends FlatSpec with Matchers with ScalaFutures {
  private val collectorConfig = ConfigFactory.load("collector.conf")
  private val replicaConfig = ConfigFactory.load("replica.conf")
  private val uuid1: UUID = UUID.randomUUID()
  private val uuid2: UUID = UUID.randomUUID()

  "CouchDB collector and data source" should "persist messages and perform replication" in {
    val masterDatabaseUtils = new DatabaseUtils(collectorConfig)
    val slaveDatabaseUtils = new DatabaseUtils(replicaConfig)

    Await.result(masterDatabaseUtils.init, Duration.Inf)
    Await.result(masterDatabaseUtils.clean, Duration.Inf)

    Await.result(slaveDatabaseUtils.init, Duration.Inf)
    Await.result(slaveDatabaseUtils.clean, Duration.Inf)

    val actorSystem = ActorSystem()
    val props = new CouchDbCollectorConstructor().propsFromConfig(collectorConfig)

    val couchDbCollector = actorSystem.actorOf(props)

    val senderMessage1 = SenderMessage(uuid1, "sender1", Some(JObject("test" -> JBool(true))))
    val senderMessage2 = SenderMessage(uuid2, "sender2", None)
    val receiverMessage = ReceiverMessage(uuid1, "receiver1")
    val messagesRelation = MessagesRelation(uuid1, uuid2)

    couchDbCollector ! senderMessage1
    couchDbCollector ! senderMessage2
    couchDbCollector ! receiverMessage
    couchDbCollector ! messagesRelation

    // Wait for operations on DBs and replication
    Thread.sleep(5000)

    Await.result(actorSystem.terminate(), Duration.Inf)

    val couchDbDataSource = new CouchDbDataSource(replicaConfig)
    val messagesFuture = couchDbDataSource.getMessages
    val relationsFuture = couchDbDataSource.getRelations
    val future = for {
      messages <- messagesFuture
      relations <- relationsFuture
    } yield (messages, relations)

    whenReady(future) {
      case (messages, relations) =>
        messages should contain(Message(
          uuid1, "sender1", Some("receiver1"), Some(JObject("test" -> JBool(true)))
        ))
        messages should contain(Message(uuid2, "sender2", None, None))
        messages should have length 2

        relations should contain(MessagesRelation(uuid1, uuid2))
        relations should have length 1
    }
  }
}
