package co.enear.fs2.kafka
package internal

import fs2._
import fs2.util.Async
import fs2.util.syntax._
import org.apache.kafka.common.TopicPartition

/**
 * Logic to subscribe / assign to topics and partitions
 *
 */
private[kafka] trait Subscriber[F[_], K, V, -S <: Subscription] {
  def subscribe(consumer: ConsumerControl[F, K, V])(subscription: S): F[Unit]
}

/**
 * Subscribe to partitions with no activity on rebalance
 *
 */
private[kafka] class SimpleSubscriber[F[_]: Async, K, V] extends Subscriber[F, K, V, Subscription] {
  override def subscribe(consumer: ConsumerControl[F, K, V])(subscription: Subscription) = subscription match {
    case s: AutoSubscription => consumer.subscribe(s, RebalanceListener.doNothing)
    case s: ManualSubscription => consumer.assign(s)
  }
}

private[kafka] trait SubscriptionEvent

private[kafka] object SubscriptionEvent {

  final case class PartitionsAssigned(partitions: Set[TopicPartition]) extends SubscriptionEvent

  final case class PartitionsRevoked(partitions: Set[TopicPartition]) extends SubscriptionEvent
}

/**
 * Subscribes to partitions putting subscription events on a queue
 *
 * Note that partitions are paused on assignment so they can be assynchronously resumed
 *  and no message are lost
 */
private[kafka] class AsyncSubscriber[F[_], K, V](queue: async.mutable.Queue[F, SubscriptionEvent])(implicit F: Async[F]) extends Subscriber[F, K, V, Subscription] {
  import SubscriptionEvent._
  private def enqueue(event: SubscriptionEvent) = {
    queue.enqueue1(event).unsafeRunAsync { _ => () }
    ()
  }
  val listener = RebalanceListener({ assigned =>
    enqueue(PartitionsAssigned(assigned.toSet))
  }, { revoked =>
    enqueue(PartitionsRevoked(revoked.toSet))
  })

  override def subscribe(consumer: ConsumerControl[F, K, V])(subscription: Subscription) = subscription match {
    case s: AutoSubscription =>
      consumer.subscribe(s, RebalanceListener.wrapePaused(consumer.rawConsumer, listener))
    case s: ManualSubscription =>
      consumer.assign(s) >> consumer.assignment.flatMap { partitions =>
        queue.enqueue1(SubscriptionEvent.PartitionsAssigned(partitions))
      }

  }
}
