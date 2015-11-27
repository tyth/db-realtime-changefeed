package org.db.changefeed

import java.util.UUID

import akka.actor.{ActorRef, PoisonPill, Props, ActorSystem}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.db.changefeed.CollectionFeedActor.{FetchRange, CleanUp, FetchAll, Increment}
import org.mongodb.scala.MongoDatabase

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

case class ChangeEvent[K, V](key: K, oldValue: Option[V], newValue: V)

case class Trigger[K, V](id: UUID, condition: (V, V) => Boolean, callback: ChangeEvent[K, V] => Unit)

trait KeyRange[K] {
  def min: K
  def max: K
  def inRange(key: K): Boolean
}

object KeyRange {
  /**
   * Returns inclusive interval [minKey, maxKey]
   */
  def apply[K : Ordering](minKey: => K, maxKey: => K) = new KeyRange[K] {
    override def min = minKey
    override def max = maxKey

    override def inRange(key: K) = {
      val o = implicitly[Ordering[K]]
      o.compare(key, min) >= 0 && o.compare(key, max) <= 0
    }
  }
}

object DateTimeRange {
  import com.github.nscala_time.time.Imports._

  /**
   * Returns inclusive dynamic interval [now - range, now]
   */
  def last(range: FiniteDuration) =  {
    def now = DateTime.now
    KeyRange[DateTime](now.minus(range.toMillis), now)
  }
}

/**
 * Facade over @CollectionFeedActor
 */
case class CollectionFeed[K: Ordering, V](actor: ActorRef)
                                         (implicit num: Numeric[V], system: ActorSystem) extends AutoCloseable {
  implicit val timeout = Timeout(10.seconds)

  /**
   * Increments value of given key, returns UUID list of activated triggers.
   */
  def increment(key: K, incrementValue: V = num.one): Future[Seq[UUID]] = {
    (actor ? Increment(key, incrementValue)).mapTo[Seq[UUID]]
  }

  /**
   * Returns sorted sequence of key-value pairs
   */
  def fetch(): Future[Seq[(K, V)]] = {
    (actor ? FetchAll).mapTo[Seq[(K, V)]]
  }

  /**
   * Returns sorted sequence of key-value pairs, restricted to inclusive interval [min, max]
   */
  def fetch(min: K, max: K): Future[Seq[(K, V)]] = {
    (actor ? FetchRange(min, max)).mapTo[Seq[(K, V)]]
  }

  override def close(): Unit = actor ! PoisonPill
}

case class MongoCollectionFeedBuilder[K: Ordering, V](name: String, database: MongoDatabase)
                                                     (implicit dbKey: MongoDbKey[K], dbValue: MongoDbValue[V],
                                                      num: Numeric[V], actorSystem: ActorSystem) {
  private val triggers = mutable.ArrayBuffer[Trigger[K, V]]()
  private var rangeOpt: Option[KeyRange[K]] = None
  private var interval: FiniteDuration = 1.minute

  private val dbCollection = database.getCollection(name)

  def range(range: KeyRange[K]) = {
    this.rangeOpt = Some(range)
    this
  }

  /**
   * Sets interval for running a cleaning task. This task removes all data about keys, that are not in the range
   * at that moment. If no range is specified, the task basically does nothing.
   */
  def cleanInterval(interval: FiniteDuration) = {
    this.interval = interval
    this
  }

  def build: Future[CollectionFeed[K, V]] = {
    import org.mongodb.scala.model.Filters._

    val findRequest = rangeOpt match {
      case Some(range) =>
        dbCollection.find(and(gte("key", range.min), lte("key", range.max)))
      case None =>
        dbCollection.find()
    }

    findRequest.toFuture().map { docs =>
      println("fetched" + docs.toString)

      val lastFetchedValues = Map[K, V]() ++
        docs.map(d => d.get("key") -> d.get("value")).collect { case (Some(key), Some(value)) =>
          dbKey.read(key) -> dbValue.read(value)
        }
      val collection = new MongoDbCollection[K, V](dbCollection)
      val actor = actorSystem.actorOf(Props(new CollectionFeedActor(collection, triggers, lastFetchedValues, rangeOpt)))

      actorSystem.scheduler.schedule(interval, interval, actor, CleanUp)
      CollectionFeed(actor)
    }
  }

  def addTrigger(condition: (V, V) => Boolean, callback: ChangeEvent[K, V] => Unit): MongoCollectionFeedBuilder[K, V] = {
    addTrigger(Trigger(UUID.randomUUID(), condition, callback))
  }

  def addTrigger(trigger: Trigger[K, V]) = {
    triggers.append(trigger)
    this
  }
}

/**
 * Starting point for constructing change feeds.
 */
case class MongoDbChangefeed(database: MongoDatabase) {
  def collection[K : Ordering : MongoDbKey, V : Numeric : MongoDbValue]
    (name: String)(implicit actorSystem: ActorSystem = ActorSystem(
                                                         s"changefeed-system-$name",
                                                         MongoDbChangefeed.defaultAkkaConfig
                                                       )) = {
    MongoCollectionFeedBuilder[K, V](name, database)
  }
}

object MongoDbChangefeed {
  val defaultAkkaConfig = ConfigFactory.parseString("akka.daemonic=on")
}