package at.yawk.numaec;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

public final class LinearHashMapConfig {
    final float loadFactor;
    final int regionSize;
    final int bucketSize;
    final int pointerSize;
    final LongSupplier sipHashK0;
    final LongSupplier sipHashK1;

    private LinearHashMapConfig(Builder builder) {
        this.loadFactor = builder.loadFactor;
        this.regionSize = builder.regionSize;
        this.bucketSize = builder.bucketSize;
        this.pointerSize = builder.pointerSize;
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

        public LinearHashMapConfig build() {
            return new LinearHashMapConfig(this);
        }
    }
}
