package com.evolutiongaming.catshelper

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import com.evolutiongaming.catshelper.IOSuite._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * `ToTry[IO]` is how skafka bridges a `RebalanceCallback` `Lift` node to the Java rebalance
 * listener: the effect is run to completion on the Kafka poll thread, bounded by the ambient
 * `ioToTry(1.minute)`.
 *
 * kafka-flow lifts all of partition recovery through that bridge (`RebalanceListener.scala:31`,
 * `flow.add` -> `TopicFlow.add` -> `PartitionFlow.acquire`), which is resource-shaped: a guarded
 * acquire completes synchronously, then the state of every key is rebuilt from the snapshot store
 * across an asynchronous boundary.
 *
 * The specs below model that shape. They are about what happens at the ceiling: whether a timeout
 * is an orderly failure the caller can retry, or an abandonment that leaves the resource's guard
 * held.
 */
class ToTryTimeoutSpec extends AnyFunSuite with Matchers {

  private val DefaultTimeout = 200.millis

  /**
   * Longer than any test is willing to wait: a passing run never reaches it.
   */
  private val RecoveryDuration = 10.seconds

  test("timeout releases a resource acquired before the async boundary") {
    val f = fixture()

    f.toTry(f.recovery) should matchPattern { case Failure(_: TimeoutException) => }

    f.events shouldEqual Vector("acquired", "released")
  }

  test("timeout waits for the release to complete before returning") {
    val f = fixture(releaseDuration = 300.millis)

    f.toTry(f.recovery)

    // read with no grace period: the release must have finished, not merely been started
    f.events shouldEqual Vector("acquired", "released")
  }

  test("a timed out attempt does not strand the guard it acquired") {
    val f = fixture()

    f.toTry(f.recovery) should matchPattern { case Failure(_: TimeoutException) => }

    // what an application-level retry (e.g. kafka-flow's flowRetry) does next
    f.toTry(f.recovery) should matchPattern { case Failure(_: TimeoutException) => }

    f.events shouldEqual Vector("acquired", "released", "acquired", "released")
  }

  test("a synchronous effect runs on the calling thread") {
    val f = fixture()

    f.toTry(IO(Thread.currentThread.getName)) shouldEqual Try(Thread.currentThread.getName)
  }

  test("a synchronous effect is not subject to the timeout") {
    val f = fixture(timeout = 1.nano)

    f.toTry(IO.unit.replicateA_(100000).as("done")) shouldEqual Try("done")
  }

  test("an uncancelable effect outlives the timeout") {
    val f = fixture()

    val (result, elapsed) = timed {
      f.toTry(IO.uncancelable(_ => IO.sleep(1.second)))
    }

    // The deadline is enforced by cancelling, and an uncancelable region cannot be cancelled.
    // Returning early instead would hand the caller a result while the effect still holds its
    // resources, which is the failure the specs above are about.
    result shouldEqual Success(())
    elapsed should be >= 1.second
  }

  test("an uncancelable guarded recovery keeps its guard across the timeout") {
    val f = fixture(timeout = 100.millis, recoveryDuration = 300.millis)

    // kafka-flow guards `add`, `apply` and the flow's own release with a single permit, and takes
    // that permit inside `uncancelable` for exactly this reason. Losing it wedges all three.
    f.toTry(f.guardedRecovery) shouldEqual Success(())
    f.toTry(f.guardedRecovery) shouldEqual Success(())

    f.events shouldEqual Vector("acquired", "released", "acquired", "released")
  }

  private def timed[A](f: => A): (A, FiniteDuration) = {
    val started = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - started).nanos)
  }

  private def fixture(
    timeout: FiniteDuration = DefaultTimeout,
    releaseDuration: FiniteDuration = Duration.Zero,
    recoveryDuration: FiniteDuration = RecoveryDuration,
  ): Fixture = {
    val log = new AtomicReference(Vector.empty[String])
    new Fixture(
      toTry0 = ToTry.ioToTry(timeout),
      guard = Semaphore[IO](1).unsafeRunSync(),
      log = log,
      releaseDuration = releaseDuration,
      recoveryDuration = recoveryDuration,
    )
  }

  private class Fixture(
    toTry0: ToTry[IO],
    guard: Semaphore[IO],
    log: AtomicReference[Vector[String]],
    releaseDuration: FiniteDuration,
    recoveryDuration: FiniteDuration,
  ) {

    def toTry[A](fa: IO[A]): Try[A] = toTry0(fa)

    def events: Vector[String] = log.get()

    /**
     * Mimics `PartitionFlow`'s acquisition: take the guard and finish the acquire synchronously,
     * then rebuild state across an async boundary. The release gives the guard back.
     */
    def recovery: IO[Unit] =
      Resource
        .make(guard.acquire *> record("acquired")) { _ =>
          IO.sleep(releaseDuration) *> record("released") *> guard.release
        }
        .use(_ => IO.sleep(recoveryDuration))

    /**
     * The same, wrapped the way `TopicFlow.safeguard` wraps it: one permit guarding the flow, and
     * `uncancelable` so that the permit cannot be lost to cancellation.
     */
    def guardedRecovery: IO[Unit] = recovery.uncancelable

    private def record(event: String): IO[Unit] = IO(log.updateAndGet(_ :+ event)).void
  }
}
