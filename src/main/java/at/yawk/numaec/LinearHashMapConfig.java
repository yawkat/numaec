package at.yawk.numaec;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

public final class LinearHashMapConfig {
    final float loadFactor;
    final int regionSize;
    final int bucketSize;
    final int pointerSize;
    final int hashLength;
    final LongSupplier sipHashK0;
    final LongSupplier sipHashK1;

    private LinearHashMapConfig(Builder builder) {
        this.loadFactor = builder.loadFactor;
        this.regionSize = builder.regionSize;
        this.bucketSize = builder.bucketSize;
        this.pointerSize = builder.pointerSize;
        this.hashLength = builder.hashLength;
        this.sipHashK0 = builder.sipHashK0;
        this.sipHashK1 = builder.sipHashK1;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        float loadFactor = 0.75f;
        int regionSize = 16;
        int bucketSize = BTreeConfig.PAGE_SIZE;
        int pointerSize = 4;
        int hashLength = 4;

        LongSupplier sipHashK0;
        LongSupplier sipHashK1;

        { generateHashKey(); }

        public Builder loadFactor(float loadFactor) {
            this.loadFactor = loadFactor;
            return this;
        }

        public Builder regionSize(int regionSize) {
            this.regionSize = regionSize;
            return this;
        }

        public Builder bucketSize(int bucketSize) {
            this.bucketSize = bucketSize;
            return this;
        }

        public Builder pointerSize(int pointerSize) {
            this.pointerSize = pointerSize;
            return this;
        }

        public Builder generateHashKey(Random rng) {
            sipHashK1 = sipHashK0 = rng::nextLong;
            return this;
        }

        public Builder generateHashKey() {
            sipHashK1 = sipHashK0 = () -> ThreadLocalRandom.current().nextLong();
            return this;
        }

        /**
         * Number of bytes to use as the hash. Lower values reduce numa memory footprint but effectively limit map size.
         */
        public Builder hashLength(int hashLength) {
            if (hashLength != 1 && hashLength != 2 && hashLength != 4 && hashLength != 8) {
                throw new UnsupportedOperationException("Unsupported hash length");
            }
            this.hashLength = hashLength;
            return this;
        }

        /**
         * Do not store the hash of entries, but instead compute it on the fly from the stored key. Lowest possible
         * numa memory footprint at cost of cpu use during both read and write operations.
         */
        public Builder dontStoreHash() {
            this.hashLength = 0;
            return this;
        }

        public LinearHashMapConfig build() {
            return new LinearHashMapConfig(this);
        }
    }
}
