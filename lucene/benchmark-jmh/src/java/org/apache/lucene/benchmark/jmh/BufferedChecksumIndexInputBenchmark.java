/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.BufferedChecksumIndexInputOrig;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3, jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class BufferedChecksumIndexInputBenchmark {

  private static final String NAME = "bufferedChecksum";
  private Path path;
  private Directory dir;
  private IndexInput in;

  @Param({"8", "16", "32", "64"})
  public int size;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory(NAME);
    dir = FSDirectory.open(path);
    try (IndexOutput out = dir.createOutput(NAME, IOContext.DEFAULT)) {
      Random r = new Random(0);
      // Write enough random data to not reach EOF while decoding
      CodecUtil.writeHeader(out, NAME, 0);
      for (int i = 0; i < size; ++i) {
        out.writeLong(r.nextLong());
      }
      CodecUtil.writeFooter(out);
    }
    in = dir.openInput(NAME, IOContext.READONCE);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (dir != null) {
      dir.deleteFile(NAME);
    }
    IOUtils.close(in, dir);
    in = null;
    dir = null;
    Files.deleteIfExists(path);
  }

  private void decodeSingleLongs(ChecksumIndexInput in, Blackhole bh) throws IOException {
    CodecUtil.checkHeader(in, NAME, 0, 0);
    long[] dst = new long[size];
    for (int i = 0; i < size; ++i) {
      dst[i] = in.readLong();
    }
    bh.consume(CodecUtil.checkFooter(in));
    bh.consume(dst);
  }

  private void decodeLongArray(ChecksumIndexInput in, Blackhole bh) throws IOException {
    CodecUtil.checkHeader(in, NAME, 0, 0);
    long[] dst = new long[size];
    in.readLongs(dst, 0, size);
    bh.consume(CodecUtil.checkFooter(in));
    bh.consume(dst);
  }

  private ChecksumIndexInput newChecksumInput() {
    return new BufferedChecksumIndexInput(in.clone());
  }

  private ChecksumIndexInput newChecksumInputOrig() {
    return new BufferedChecksumIndexInputOrig(in.clone());
  }

  @Benchmark
  public void decodeSingleLongs(Blackhole bh) throws IOException {
    decodeSingleLongs(newChecksumInput(), bh);
  }

  //  @Benchmark
  //  public void decodeSingleLongsOrig(Blackhole bh) throws IOException {
  //    decodeSingleLongs(newChecksumInputOrig(), bh);
  //  }

  @Benchmark
  public void decodeLongArray(Blackhole bh) throws IOException {
    decodeLongArray(newChecksumInput(), bh);
  }

  //  @Benchmark
  //  public void decodeLongArrayOrig(Blackhole bh) throws IOException {
  //    decodeSingleLongs(newChecksumInputOrig(), bh);
  //  }
}
