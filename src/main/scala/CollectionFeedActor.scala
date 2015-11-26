package org.db.changefeed

import java.util.UUID

import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.pattern.pipe
import org.db.changefeed.CollectionFeedActor._

import scala.collection.mutable
import scala.util.Try

class CollectionFeedActor[K: Ordering, V](collection: Collection[K, V],
                                          triggers: Seq[Trigger[K, V]],
                                          values: Map[K, V],
                                          range: Option[KeyRange[K]])
                                         (implicit num: Numeric[V]) extends Actor with ActorLogging {
  import context.dispatcher

  val latestValues = mutable.Map() ++ values
  val lastFetchedValues = triggers.map { t =>
    t.id -> (mutable.Map[K, V]() ++ latestValues)
  }.toMap

  override def receive: Receive = {
    case Increment(anyKey: Any, anyValue: Any) =>
      convertType(anyKey, anyValue).foreach { case (key, incrementValue) =>
        val origin = sender()
        val incrementF = collection.increment(key, incrementValue)

        if (inRange(key)) {
          incrementF.map(_ => OnIncrement(key, incrementValue, origin)).pipeTo(self)
        } else {
          // return empty list of triggered IDs
          origin ! Seq.empty[Option[UUID]]
        }
      }
    case OnIncrement(anyKey: Any, anyValue: Any, origin: ActorRef) =>
      convertType(anyKey, anyValue).foreach { case (key, incrementValue) =>

        val newValue = num.plus(latestValues.getOrElse(key, num.zero), incrementValue)
        latestValues.update(key, newValue)

        origin ! triggers.flatMap { trigger =>
          val oldValue = lastFetchedValues(trigger.id).get(key)
          val shouldBeTriggered = oldValue match {
            case Some(old) => trigger.condition(old, newValue)
            case _ => true
          }

          if (shouldBeTriggered) {
            trigger.callback(ChangeEvent(key, oldValue, newValue))
            lastFetchedValues(trigger.id).update(key, newValue)
            Some(trigger.id)
          } else {
            None
          }
        }
      }
    case Fetch =>
      sender ! latestValues.toSeq.sortBy(_._1)
    case CleanUp =>
      cleanValues(latestValues)
      lastFetchedValues.foreach { case (_, vals) =>
        cleanValues(vals)
      }
      sender ! Unit
  }

  @unchecked
  private def convertType(anyKey: Any, anyValue: Any) = Try {
    anyKey.asInstanceOf[K] -> anyValue.asInstanceOf[V]
  }.recover {
    case e: ClassCastException =>
      log.error("Received message with wrong types", e)
      throw e
  }

  private def inRange(key: K) = range.map(_.inRange(key)).getOrElse(true)

  private def cleanValues(values: mutable.Map[K, V]) = values.foreach { case (key, _) =>
    if (!inRange(key)) values.remove(key)
  }
}


object CollectionFeedActor {
  case class Increment[K: Ordering, V](key: K, inc: V)
  case class OnIncrement[K: Ordering, V](key: K, inc: V, origin: ActorRef)
  case object Fetch
  case object CleanUp
}
