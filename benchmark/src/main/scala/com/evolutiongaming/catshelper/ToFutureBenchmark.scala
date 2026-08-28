package com.evolutiongaming.catshelper

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
 * The same question as [[ToTryBenchmark]], asked of `ToFuture[IO]`.
 *
 * `syncStep` walks the computation and hands back an already completed `Future` when it has no
 * asynchronous boundary; `runtime` always schedules it. Unlike `ToTry`, neither blocks the caller
 * waiting for a result, so the difference should be the cost of scheduling alone rather than of a
 * round trip.
 *
 * `handOff` measures what the caller pays to obtain the `Future`, `endToEnd` what it pays to have
 * the value.
 *
 * To run: {{{sbt "benchmark/Jmh/run com.evolutiongaming.catshelper.ToFutureBenchmark"}}}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
class ToFutureBenchmark {

  @Param(Array("syncStep", "runtime"))
  var implementation: String = ""

  /** Chained synchronous stages, standing in for the work a codec does per record. */
  @Param(Array("1", "64"))
  var stages: Int = 0

  // JMH drives state through mutable fields and lifecycle hooks, so `var` is required here.
  private var toFuture: ToFuture[IO] = null
  private var synchronous: IO[Int] = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    implicit val runtime: IORuntime = IORuntime.global

    toFuture = implementation match {
      case "syncStep" => ToFutureBenchmark.syncStep
      case "runtime"  => ToFutureBenchmark.runtime
      case other      => throw new IllegalArgumentException(other)
    }

    synchronous = (1 to stages).foldLeft(IO.delay(1)) { (io, n) => io.map(_ * 31 + n) }
  }

  @Benchmark
  def handOff(hole: Blackhole): Unit = hole.consume(toFuture(synchronous))

  @Benchmark
  def endToEnd(hole: Blackhole): Unit =
    hole.consume(Await.result(toFuture(synchronous), 1.minute))
}

object ToFutureBenchmark {

  /** As on master: walk the computation, and only schedule what is left. */
  def syncStep(implicit runtime: IORuntime): ToFuture[IO] = new ToFuture[IO] {
    def apply[A](fa: IO[A]): Future[A] =
      Try(fa.syncStep(Int.MaxValue).unsafeRunSync()) match {
        case Success(Left(remainder)) => remainder.unsafeToFuture()
        case Success(Right(a))        => Future.successful(a)
        case Failure(error)           => Future.failed(error)
      }
  }

  /** No walk: schedule the whole computation. */
  def runtime(implicit runtime: IORuntime): ToFuture[IO] = new ToFuture[IO] {
    def apply[A](fa: IO[A]): Future[A] = fa.unsafeToFuture()
  }
}
