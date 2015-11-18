package org.db.changefeed

import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.pattern.pipe
import org.db.changefeed.CollectionFeedActor._

import scala.collection.mutable
import scala.util.Try

class CollectionFeedActor[K: Ordering, V](collection: Collection[K, V], triggers: Seq[Trigger[K, V]], values: Map[K, V])(implicit num: Numeric[V]) extends Actor with ActorLogging {
  import context.dispatcher

  val latestValues = mutable.Map() ++ values
  val lastFetchedValues = triggers.map { t =>
    t.id -> (mutable.Map[K, V]() ++ latestValues)
  }.toMap

  override def receive: Receive = {
    case Increment(tsAny: Any, incrementValueAny: Any) =>
      convertType(tsAny, incrementValueAny).foreach { case (ts, incrementValue) =>
        val origin = sender()
        collection.increment(ts, incrementValue).map(_ => OnIncrement(ts, incrementValue, origin)).pipeTo(self)
      }
    case OnIncrement(tsAny: Any, incrementValueAny: Any, origin: ActorRef) =>
      convertType(tsAny, incrementValueAny).foreach { case (ts, incrementValue) =>

        val newValue = num.plus(latestValues.getOrElse(ts, num.zero), incrementValue)
        latestValues.update(ts, newValue)

        origin ! triggers.flatMap { trigger =>
          val oldValue = lastFetchedValues(trigger.id).get(ts)
          val shouldBeTriggered = oldValue match {
            case Some(old) => trigger.condition(old, newValue)
            case _ => true
          }

          if (shouldBeTriggered) {
            trigger.callback(ChangeEvent(ts, oldValue, newValue))
            lastFetchedValues(trigger.id).update(ts, newValue)
            Some(trigger.id)
          } else {
            None
          }
        }
      }
    case Fetch =>
      sender ! latestValues.toSeq.sortBy(_._1)
  }

  @unchecked
  private def convertType(tsAny: Any, incrementValueAny: Any) = Try {
    tsAny.asInstanceOf[K] -> incrementValueAny.asInstanceOf[V]
  }.recover {
    case e: ClassCastException =>
      log.error("Received message with wrong types", e)
      throw e
  }
}

object CollectionFeedActor {
  case class Increment[K: Ordering, V](ts: K, inc: V)
  case class OnIncrement[K: Ordering, V](ts: K, inc: V, origin: ActorRef)
  case object Fetch
}
