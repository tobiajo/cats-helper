package com.evolutiongaming.catshelper

import cats.effect.kernel.{CancelScope, MonadCancel}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Sync, SyncIO}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.duration._
import scala.util.Try

/**
 * Cost of `ToTry[IO]` on the two shapes it is handed in practice, for three ways of implementing
 * it.
 *
 * `syncStep` walks the computation into `SyncIO` and only reaches the runtime at the first
 * asynchronous boundary, so a computation that has none never leaves the calling thread. That is
 * the fast path kafka-journal takes per record, and it is also why a deadline cannot unwind what it
 * interrupts: stepping into `SyncIO`, whose root cancel scope is uncancelable, unwraps
 * `IO.uncancelable` and drops `IO.onCancel` along the way.
 *
 * `runtime` gives up the walk and runs everything as a fiber, which is correct but pays scheduling
 * on every conversion. `cancelableSyncStep` keeps the walk but performs it with a cancelable root,
 * so the interpreter stops at the first `uncancelable` or `onCancel` node instead of stripping it.
 *
 * The question this answers is what the fast path is worth: how much throughput separates
 * `syncStep` from `runtime` on `synchronous`, and how closely `cancelableSyncStep` recovers it.
 * `asyncBoundary` is the control — all three must reach the runtime there, so their scores should
 * meet.
 *
 * To run: {{{sbt "benchmark/Jmh/run com.evolutiongaming.catshelper.ToTryBenchmark"}}}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
class ToTryBenchmark {

  @Param(Array("syncStep", "runtime", "cancelableSyncStep"))
  var implementation: String = ""

  /** Chained synchronous stages, standing in for the work a codec does per record. */
  @Param(Array("1", "64"))
  var stages: Int = 0

  // JMH drives state through mutable fields and lifecycle hooks, so `var` is required here.
  private var toTry: ToTry[IO] = null
  private var synchronous: IO[Int] = null
  private var asyncBoundary: IO[Int] = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    implicit val runtime: IORuntime = IORuntime.global

    toTry = implementation match {
      case "syncStep"           => ToTryBenchmark.syncStep(1.minute)
      case "runtime"            => ToTryBenchmark.runtime(1.minute)
      case "cancelableSyncStep" => ToTryBenchmark.cancelableSyncStep(1.minute)
      case other                => throw new IllegalArgumentException(other)
    }

    synchronous = (1 to stages).foldLeft(IO.delay(1)) { (io, n) => io.map(_ * 31 + n) }
    asyncBoundary = IO.cede *> synchronous
  }

  @Benchmark
  def synchronousShape(hole: Blackhole): Unit = hole.consume(toTry(synchronous))

  @Benchmark
  def asyncBoundaryShape(hole: Blackhole): Unit = hole.consume(toTry(asyncBoundary))
}

object ToTryBenchmark {

  /** As on master: walk into `SyncIO`, then abandon whatever is left when the deadline passes. */
  def syncStep(timeout: FiniteDuration)(implicit runtime: IORuntime): ToTry[IO] = new ToTry[IO] {
    def apply[A](fa: IO[A]): Try[A] =
      Try {
        fa.syncStep(Int.MaxValue).unsafeRunSync() match {
          case Right(a) => a
          case Left(remainder) =>
            remainder
              .unsafeRunTimed(timeout)
              .getOrElse(throw new TimeoutException(timeout.toString))
        }
      }
  }

  /** No walk: run the whole computation as a fiber, bounded by `IO.timeout`. */
  def runtime(timeout: FiniteDuration)(implicit runtime: IORuntime): ToTry[IO] = new ToTry[IO] {
    def apply[A](fa: IO[A]): Try[A] = Try(fa.timeout(timeout).unsafeRunSync())
  }

  /** Walk with a cancelable root, so the interpreter stops at cancellation structure. */
  def cancelableSyncStep(timeout: FiniteDuration)(implicit runtime: IORuntime): ToTry[IO] =
    new ToTry[IO] {
      def apply[A](fa: IO[A]): Try[A] =
        Try {
          IO.asyncForIO.syncStep[SyncIO, A](fa, Int.MaxValue)(cancelableSyncIO).unsafeRunSync() match {
            case Right(a)        => a
            case Left(remainder) => remainder.timeout(timeout).unsafeRunSync()
          }
        }
    }

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
    def handleErrorWith[A](fa: SyncIO[A])(f: Throwable => SyncIO[A]): SyncIO[A] =
      delegate.handleErrorWith(fa)(f)
    def flatMap[A, B](fa: SyncIO[A])(f: A => SyncIO[B]): SyncIO[B] = delegate.flatMap(fa)(f)
    def tailRecM[A, B](a: A)(f: A => SyncIO[Either[A, B]]): SyncIO[B] = delegate.tailRecM(a)(f)
  }
}
