package co.enear.fs2.kafka
package internal

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

/**
 * Subscribes to partitions notifying provided callbacks
 *
 * Note that partitions are paused on assignment so they can be assynchronously resumed
 *  and no message is lost
 */
private[kafka] class AsyncSubscriber[F[_], K, V](onAssignedCB: Set[TopicPartition] => F[Unit], onRevokedCB: Set[TopicPartition] => F[Unit])(implicit F: Async[F]) extends Subscriber[F, K, V, Subscription] {

  val listener = RebalanceListener({ assigned =>
    onAssignedCB(assigned.toSet).unsafeRunAsync { _ => () }
  }, { revoked =>
    onRevokedCB(revoked.toSet).unsafeRunAsync { _ => () }
  })

  override def subscribe(consumer: ConsumerControl[F, K, V])(subscription: Subscription) = subscription match {
    case s: AutoSubscription =>
      consumer.subscribe(s, RebalanceListener.wrapePaused(consumer.rawConsumer, listener))
    case s: ManualSubscription =>
      consumer.assign(s) >> consumer.assignment.flatMap { partitions =>
        consumer.pause(partitions) >>
          onAssignedCB(partitions)
      }

  }
}
