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
package org.apache.lucene.codecs.bloom;

import org.apache.lucene.util.BytesRef;

public class MurmurHash64 extends HashFunction {
  private static final long M64 = 0xc6a4a7935bd1e995L;
  private static final int R64 = 47;
  public static final HashFunction INSTANCE = new MurmurHash64();

  /**
   * Generates a 64-bit hash from byte array of the given length and seed.
   *
   * @param data The input byte array
   * @param seed The initial seed value
   * @param length The length of the array
   * @return The 64-bit hash of the given array
   */
  public static long hash64(byte[] data, int seed, int offset, int length) {
    long h = (seed & 0xffffffffL) ^ (length * M64);

    final int nblocks = length >> 3;

    // body
    for (int i = 0; i < nblocks; i++) {

      long k = getLittleEndianLong(data, offset);
      k *= M64;
      k ^= k >>> R64;
      k *= M64;

      h ^= k;
      h *= M64;

      offset += Long.BYTES;
    }

    int remaining = length & 0x07;
    if (0 < remaining) {
      for (int i = 0; i < remaining; i++) {
        h ^= ((long) data[offset + i] & 0xff) << (Byte.SIZE * i);
      }
      h *= M64;
    }

    h ^= h >>> R64;
    h *= M64;
    h ^= h >>> R64;

    return h;
  }

  /**
   * Gets the little-endian long from 8 bytes starting at the specified index.
   *
   * <p>from Apache commons MurmurHash2.
   *
   * @param data The data
   * @param index The index
   * @return The little-endian long
   */
  private static long getLittleEndianLong(final byte[] data, final int index) {
    return (((long) data[index] & 0xff))
        | (((long) data[index + 1] & 0xff) << 8)
        | (((long) data[index + 2] & 0xff) << 16)
        | (((long) data[index + 3] & 0xff) << 24)
        | (((long) data[index + 4] & 0xff) << 32)
        | (((long) data[index + 5] & 0xff) << 40)
        | (((long) data[index + 6] & 0xff) << 48)
        | (((long) data[index + 7] & 0xff) << 56);
  }

  @Override
  public final long hash(BytesRef br) {
    return hash64(br.bytes, 0xe17a1465, br.offset, br.length);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
