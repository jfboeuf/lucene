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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat.BloomFilteredFieldsProducer.BloomFilteredTermsEnum;
import org.apache.lucene.codecs.bloom.FuzzySet.ContainsResult;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

final class BloomBench {
  private static final String FIELD_NAME = "myField";
  private static final int DOC_COUNT = 10_000_000;
  private static final int TERM_LENGTH = 9;

  private static final int ITERATIONS = 10;
  private static final int LOOKUP_PER_ITERATION = 1_000_000;

  public static void main(String[] args) throws IOException, InterruptedException {
    byte[] data = createTermData();
    System.out.println();
    for (int i = 0; i < ITERATIONS; i++) {
      boolean countCollision = 0 == i;
      benchHashFunction(MurmurHash2.INSTANCE, data, countCollision);
      benchHashFunction(MurmurHash64.INSTANCE, data, countCollision);
      System.out.println();
    }
    compareCollisions(data);
    BloomBench[] tests = new BloomBench[5];
    tests[0] = new BloomBench("Standard Lucene 94", new Lucene94Codec(), data);
    {
      Lucene94Codec bf =
          new Lucene94Codec() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
              return new BloomFilteringPostingsFormat(super.getPostingsFormatForField(field));
            }
          };
      tests[1] = new BloomBench("Optimal Bloom filter with fpp=0.1", bf, data);
    }
    {
      Lucene94Codec bf =
          new Lucene94Codec() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
              return new BloomFilteringPostingsFormat(
                  super.getPostingsFormatForField(field),
                  new DefaultBloomFilterFactory() {
                    @Override
                    public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
                      return FuzzySet.createOptimalSet(state.segmentInfo.maxDoc(), 0.2f);
                    }
                  });
            }
          };
      tests[2] = new BloomBench("Optimal Bloom filter with fpp=0.2", bf, data);
    }
    {
      Lucene94Codec bf =
          new Lucene94Codec() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
              return new BloomFilteringPostingsFormat(
                  super.getPostingsFormatForField(field),
                  new DefaultBloomFilterFactory() {
                    @Override
                    public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
                      return FuzzySet.createSetBasedOnQuality(
                          state.segmentInfo.maxDoc(), 0.1f, FuzzySet.VERSION_MURMUR2);
                    }
                  });
            }
          };
      tests[3] = new BloomBench("Original Bloom filter (k=1, MurmurHash2)", bf, data);
    }

    {
      Lucene94Codec bf =
          new Lucene94Codec() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
              return new BloomFilteringPostingsFormat(
                  super.getPostingsFormatForField(field),
                  new DefaultBloomFilterFactory() {
                    @Override
                    public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
                      return FuzzySet.createSetBasedOnQuality(
                          state.segmentInfo.maxDoc(), 0.1f, FuzzySet.VERSION_CURRENT);
                    }
                  });
            }
          };
      tests[4] = new BloomBench("Bloom filter (k=1, MurmurHash64)", bf, data);
    }

    for (BloomBench b : tests) {
      b.populate();
      Thread.sleep(10_000);
    }

    for (int i = 0; i < ITERATIONS; i++) {
      for (BloomBench b : tests) {
        b.runTest(0 == i);
      }
      System.out.println();
    }
  }

  private static void compareCollisions(byte[] termsData) throws IOException {
    System.out.print("Evaluate collisions...");
    FuzzySet optimal = FuzzySet.createOptimalSet(DOC_COUNT / 10, 0.1f);
    FuzzySet original =
        FuzzySet.createSetBasedOnQuality(DOC_COUNT / 10, 0.1f, FuzzySet.VERSION_CURRENT);
    int optimalCount = countCollistion(optimal, termsData);
    int originalCount = countCollistion(original, termsData);
    System.out.println(" original=" + originalCount + ", optimal=" + optimalCount);
    System.out.println("original: " + original);
    System.out.println("optimal: " + optimal);
  }

  private static int countCollistion(FuzzySet bloom, byte[] termsData) throws IOException {

    BytesRef br = new BytesRef(termsData, 0, TERM_LENGTH);
    for (int i = 0; i < DOC_COUNT / 10; i++) {
      br.offset = i;
      bloom.addValue(br);
    }
    int collisions = 0;
    for (int i = DOC_COUNT / 10; i < DOC_COUNT; i++) {
      br.offset = i;
      if (ContainsResult.MAYBE == bloom.contains(br)) collisions++;
    }
    return collisions;
  }

  private static void benchHashFunction(
      HashFunction function, byte[] termsData, boolean countCollisions) {
    long x = 0;
    BytesRef br = new BytesRef(termsData, 0, TERM_LENGTH);
    if (countCollisions) {
      System.out.println("warmup");
      for (int termlen = 1; termlen < TERM_LENGTH; termlen++) {
        br.length = termlen;
        for (int i = 0; i < DOC_COUNT; i++) {
          br.offset = i;
          long h = function.hash(br);
          x ^= h;
        }
      }
    }
    br.length = TERM_LENGTH;

    System.out.print("Benching " + function + "... \t");

    BitSet allHashes = countCollisions ? new FixedBitSet(Integer.MAX_VALUE) : null;
    long t0 = System.nanoTime();
    int collisions = 0;
    for (int i = 0; i < DOC_COUNT; i++) {
      br.offset = i;
      long h = function.hash(br);
      if (countCollisions && allHashes.getAndSet((int) h & Integer.MAX_VALUE)) collisions++;
      x ^= h;
    }
    if (countCollisions) System.out.print(collisions + " collisions on 31 lsb, ");
    System.out.println(" duration=" + (System.nanoTime() - t0) / 1000000. + " msec x=" + x);
  }

  private static byte[] createTermData() {
    System.out.println("\nCreate term data...");
    final Random r = new Random(42);
    final int numBytes = DOC_COUNT + TERM_LENGTH;
    final byte[] termData = new byte[numBytes];
    for (int i = 0; i < numBytes; i++) {
      termData[i] = (byte) r.nextInt(128);
    }
    return termData;
  }

  private final String label;
  private final Directory directory;
  private final Codec codec;
  private final byte[] termData;
  private final Random random = new Random(611953);

  BloomBench(String label, Codec codec, byte[] termData) throws IOException {
    this.directory = FSDirectory.open(Paths.get("/home/jboeuf/indexes/" + label));
    this.codec = codec;
    this.termData = termData;
    this.label = label;
  }

  void populate() throws IOException {
    System.out.println("\nCreate index with codec " + label);
    long start = System.currentTimeMillis();
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setRAMBufferSizeMB(256)
            .setCodec(codec);
    BytesRef br = new BytesRef(termData, 0, TERM_LENGTH);
    try (IndexWriter w = new IndexWriter(directory, config)) {

      Document doc = new Document();
      StringField field = new StringField(FIELD_NAME, "", Store.NO);
      doc.add(field);

      for (int i = 0; i < DOC_COUNT; i++) {
        br.offset = i;
        field.setStringValue(br.utf8ToString());
        w.addDocument(doc);
      }
      w.commit();
    }
    long diff = System.currentTimeMillis() - start;
    System.out.println("Index creation using " + label + " took " + diff + " ms");
  }

  void runTest(boolean displayBloom) throws IOException {
    BytesRef br = new BytesRef(termData, 0, TERM_LENGTH);
    if (displayBloom) System.out.println("\nBloom details for " + label);
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      List<LeafReaderContext> subreaders = reader.leaves();
      TermsEnum[] termsEnums = new TermsEnum[subreaders.size()];
      for (int i = 0; i < termsEnums.length; i++) {
        termsEnums[i] = subreaders.get(i).reader().terms(FIELD_NAME).iterator();
        if (displayBloom && termsEnums[i] instanceof BloomFilteredTermsEnum) {
          System.out.println("* " + termsEnums[i].toString());
        }
      }

      System.out.print("Test with codec " + label + "...");
      long t0 = System.nanoTime();

      for (int t = 0; t < LOOKUP_PER_ITERATION; t++) {
        final int idx = random.nextInt(DOC_COUNT);
        br.offset = idx;
        boolean termFound = false;
        for (int j = 0; j < termsEnums.length; j++) {
          TermsEnum termsEnum = termsEnums[j];
          if (termsEnum.seekExact(br)) {
            termFound = true;
            break;
          }
        }
        assert termFound;
      }
      System.out.println("  " + (System.nanoTime() - t0) / 1000000. + " msec");
    }
  }
}
