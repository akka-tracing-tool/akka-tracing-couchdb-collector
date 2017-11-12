package pl.edu.agh.iet.akka_tracing.collector

import java.util.UUID

import akka.actor.Props
import com.typesafe.config.Config
import pl.edu.agh.iet.akka_tracing.couchdb.model._
import pl.edu.agh.iet.akka_tracing.model.{ MessagesRelation, ReceiverMessage, SenderMessage }
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils

import scala.collection.mutable

class CouchDbCollector(config: Config) extends Collector {

  private[akka_tracing] val databaseUtils = new DatabaseUtils(config)

  import CouchDbCollector.SendToDatabase
  import pl.edu.agh.iet.akka_tracing.config.ConfigUtils._
  import databaseUtils._

  import scala.concurrent.duration._

  private val senderMessagesQueue = mutable.MutableList[SenderMessage]()
  private val receiverMessagesQueue = mutable.MutableList[ReceiverMessage]()
  private val messagesRelationsQueue = mutable.MutableList[MessagesRelation]()
  private val sendToDatabaseInterval = config.getOrElse[Int]("sendToDatabaseInterval.millis", 1000).millis

  context.system.scheduler.schedule(sendToDatabaseInterval, sendToDatabaseInterval, self, SendToDatabase)

  override def handleSenderMessage(msg: SenderMessage): Unit = {
    senderMessagesQueue += msg
  }

  override def handleReceiverMessage(msg: ReceiverMessage): Unit = {
    receiverMessagesQueue += msg
  }

  override def handleRelationMessage(msg: MessagesRelation): Unit = {
    messagesRelationsQueue += msg
  }

  override def handleOtherMessages: PartialFunction[Any, Unit] = {
    case SendToDatabase =>
      val senderMessages = senderMessagesQueue.toList
      senderMessagesQueue.clear()
      val receiverMessages = receiverMessagesQueue.toList
      receiverMessagesQueue.clear()
      val messagesRelations = messagesRelationsQueue.toList
      messagesRelationsQueue.clear()
      updatesDatabase.putDocs(List(Update(
        UUID.randomUUID.toString, receiverMessages, senderMessages, messagesRelations
      )))
  }
}

object CouchDbCollector {

  case object SendToDatabase

}

class CouchDbCollectorConstructor extends CollectorConstructor {
  override def propsFromConfig(config: Config) =
    Props(classOf[CouchDbCollector], config)
}
