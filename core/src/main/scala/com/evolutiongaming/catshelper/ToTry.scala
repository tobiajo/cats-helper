package com.evolutiongaming.catshelper

import cats.Id
import cats.arrow.FunctionK
import cats.effect.kernel.{CancelScope, MonadCancel}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Sync, SyncIO}

import scala.concurrent.duration._
import scala.util.{Success, Try}

trait ToTry[F[_]] {

  def apply[A](fa: F[A]): Try[A]
}

object ToTry {

  def apply[F[_]](
    implicit
    F: ToTry[F],
  ): ToTry[F] = F

  def summon[F[_]](
    implicit
    F: ToTry[F],
  ): ToTry[F] = F

  def functionK[F[_]: ToTry]: FunctionK[F, Try] = new FunctionK[F, Try] {

    def apply[A](fa: F[A]): Try[A] = ToTry.summon[F].apply(fa)
  }

  /**
   * Please think twice before using this, ideally you should not have toTry in your `pure` code
   * base!
   *
   * Note: There was an interesting discussion about `SyncIO` in Cats Effect
   * (https://github.com/typelevel/cats-effect/issues/4337)
   *
   * @param timeout
   *   deadline for the part of the computation that cannot be evaluated synchronously, covering all
   *   computations onwards. In case there is no such part, timeout is not used. It is enforced by
   *   cancelling the computation, so finalizers run and resources acquired before the deadline are
   *   released before this returns. A computation stuck in an uncancelable region cannot be
   *   interrupted and will outlive the deadline: returning without it would hand the caller a
   *   result while the computation still holds its resources.
   */
  def ioToTry(
    timeout: FiniteDuration,
  )(implicit
    runtime: IORuntime,
  ): ToTry[IO] = new ToTry[IO] {
    def apply[A](fa: IO[A]): Try[A] =
      Try {
        syncStep(fa).unsafeRunSync() match {
          case Right(value) => value
          case Left(remainder) => remainder.timeout(timeout).unsafeRunSync()
        }
      }
  }

  implicit def ioToTry(
    implicit
    ioRuntime: IORuntime,
  ): ToTry[IO] = ioToTry(1.minute)

  implicit val idToTry: ToTry[Id] = new ToTry[Id] {
    def apply[A](fa: Id[A]): Try[A] = Success(fa)
  }

  implicit val tryToTry: ToTry[Try] = new ToTry[Try] {
    def apply[A](fa: Try[A]): Try[A] = fa
  }

  /**
   * `fa.syncStep` interprets into `SyncIO`, whose root cancel scope is uncancelable. Under that
   * scope the interpreter unwraps `IO.uncancelable` and drops `IO.onCancel` finalizers as it walks
   * the computation, so the remainder it returns at the first asynchronous boundary has lost the
   * cancellation structure of the part that already ran: a resource whose acquire completed
   * synchronously can no longer be released.
   *
   * Stepping with a cancelable root instead stops at the first `uncancelable` or `onCancel` node
   * and returns the computation from there on untouched, which keeps the synchronous fast path for
   * computations that have no cancellation structure to lose.
   */
  private def syncStep[A](fa: IO[A]): SyncIO[Either[IO[A], A]] =
    IO.asyncForIO.syncStep[SyncIO, A](fa, Int.MaxValue)(cancelableSyncIO)

  private val cancelableSyncIO: Sync[SyncIO] = new Sync[SyncIO]
    with MonadCancel.Uncancelable[SyncIO, Throwable] {

    private val delegate = SyncIO.syncForSyncIO

    override def rootCancelScope: CancelScope = CancelScope.Cancelable

    def suspend[A](hint: Sync.Type)(thunk: => A): SyncIO[A] = delegate.suspend(hint)(thunk)
    def monotonic: SyncIO[FiniteDuration] = delegate.monotonic
    def realTime: SyncIO[FiniteDuration] = delegate.realTime
    def forceR[A, B](fa: SyncIO[A])(fb: SyncIO[B]): SyncIO[B] = delegate.forceR(fa)(fb)
    def pure[A](a: A): SyncIO[A] = delegate.pure(a)
    def raiseError[A](e: Throwable): SyncIO[A] = delegate.raiseError(e)
    def handleErrorWith[A](fa: SyncIO[A])(f: Throwable => SyncIO[A]): SyncIO[A] = delegate.handleErrorWith(fa)(f)
    def flatMap[A, B](fa: SyncIO[A])(f: A => SyncIO[B]): SyncIO[B] = delegate.flatMap(fa)(f)
    def tailRecM[A, B](a: A)(f: A => SyncIO[Either[A, B]]): SyncIO[B] = delegate.tailRecM(a)(f)
  }
}
