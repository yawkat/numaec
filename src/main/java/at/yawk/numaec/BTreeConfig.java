package at.yawk.numaec;

import java.util.StringJoiner;

public final class BTreeConfig {
    final int blockSize;
    final int regionSize;
    final int pointerSize;
    final boolean storeNextPointer;
    final boolean entryMustBeInLeaf;

    private BTreeConfig(Builder builder) {
        this.blockSize = builder.blockSize;
        this.regionSize = builder.regionSize;
        this.pointerSize = builder.pointerSize;
        this.storeNextPointer = builder.storeNextPointer;
        this.entryMustBeInLeaf = builder.entryMustBeInLeaf;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BTreeConfig.class.getSimpleName() + "[", "]")
                .add("blockSize=" + blockSize)
                .add("pointerSize=" + pointerSize)
                .add("storeNextPointer=" + storeNextPointer)
                .add("entryMustBeInLeaf=" + entryMustBeInLeaf)
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int blockSize = 4096; // TODO
        private int pointerSize = 4;
        private int regionSize = 16;
        private boolean storeNextPointer = true;
        private boolean entryMustBeInLeaf = true;

        private Builder() {
        }

        /**
         * Size of individual btree blocks. Usually the OS page size.
         */
        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Size of allocation regions in units of {@link #blockSize}. The btree will allocate buffers of {@code
         * blockSize * regionSize} bytes when additional space is needed.
         */
        public Builder regionSize(int regionSize) {
            this.regionSize = regionSize;
            return this;
        }

        /**
         * Size of block indices in the btree. Determines maximum capacity of the tree.
         */
        public Builder pointerSize(int pointerSize) {
            this.pointerSize = pointerSize;
            return this;
        }

        /**
         * Whether to store a pointer to the next leaf in every btree leaf. When {@link #entryMustBeInLeaf} is on, this
         * leads to faster iteration.
         */
        public Builder storeNextPointer(boolean storeNextPointer) {
            this.storeNextPointer = storeNextPointer;
            return this;
        }

        /**
         * Whether all values should be stored in the leaves (B+-tree instead of B-Tree). Usually a good idea.
         */
        public Builder entryMustBeInLeaf(boolean entryMustBeInLeaf) {
            this.entryMustBeInLeaf = entryMustBeInLeaf;
            return this;
        }

        public BTreeConfig build() {
            return new BTreeConfig(this);
        }
    }
}
