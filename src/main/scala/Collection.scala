package org.db.changefeed

import org.joda.time.DateTime
import org.mongodb.scala.bson.{BsonValue, BsonDateTime}

import org.mongodb.scala.model.{Updates, UpdateOptions}
import org.mongodb.scala.MongoCollection
import scala.concurrent.Future
import org.mongodb.scala.model.Filters._

import scala.concurrent.ExecutionContext.Implicits.global

trait Collection[K, V] {
  def increment(key: K, inc: V): Future[Boolean]
}

trait MongoDbKey[T] {
  def write(key: T): Any
  def read(key: BsonValue): T
}

trait MongoDbValue[T] {
  def write(value: T): Number
  def read(value: BsonValue): T
}

trait CommonImplicits {
  implicit val DateTimeOrdering = com.github.nscala_time.time.OrderingImplicits.DateTimeOrdering
}

object MongoDbImplicits extends CommonImplicits {
  implicit val DateTimeKey = new MongoDbKey[DateTime] {
    override def write(key: DateTime): BsonDateTime = BsonDateTime(key.getMillis)
    override def read(key: BsonValue): DateTime = new DateTime(key.asDateTime().getValue)
  }

  implicit val LongValue = new MongoDbValue[Long] {
    override def write(value: Long): Number = Long.box(value)
    override def read(value: BsonValue): Long = value.asNumber().longValue()
  }

  implicit val DoubleValue = new MongoDbValue[Double] {
    override def write(value: Double): Number = Double.box(value)
    override def read(value: BsonValue): Double = value.asNumber().doubleValue()
  }
}

class MongoDbCollection[K, V](collection: MongoCollection[_])
                             (implicit dbKey: MongoDbKey[K], dbValue: MongoDbValue[V]) extends Collection[K, V] {
  override def increment(key: K, inc: V): Future[Boolean] = {
    collection.updateOne(
      equal("key", dbKey.write(key)), Updates.inc("value", dbValue.write(inc)), UpdateOptions().upsert(true)
    ).toFuture().map(_.exists(_.wasAcknowledged()))
  }
}
