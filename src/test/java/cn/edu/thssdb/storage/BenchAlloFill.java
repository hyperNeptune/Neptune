package cn.edu.thssdb.storage;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;

@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"-Xms256m", "-Xmx256m", "-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchAlloFill {

  @Param({"10000", "100000", "1000000", "10000000", "100000000"})
  private int size;

  private int[] arr;

  @Setup
  public void setup() {
    arr = new int[size];
    Arrays.fill(arr, 1);
  }

  @Benchmark
  public void recreateArray() {
    arr = new int[arr.length];
  }
}
