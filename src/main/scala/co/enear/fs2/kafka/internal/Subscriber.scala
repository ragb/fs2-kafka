package co.enear.fs2.kafka
package internal

import fs2.util.Async

private[kafka] trait Subscriber[F[_], K, V, -S <: Subscription] {
  def subscribe(consumer: ConsumerControl[F, K, V])(subscription: S): F[Unit]
}

private[kafka] class SimpleSubscriber[F[_]: Async, K, V] extends Subscriber[F, K, V, Subscription] {
  override def subscribe(consumer: ConsumerControl[F, K, V])(subscription: Subscription) = subscription match {
    case s: AutoSubscription => consumer.subscribe(s, RebalanceListener.doNothing)
    case s: ManualSubscription => consumer.assign(s)
  }
}

