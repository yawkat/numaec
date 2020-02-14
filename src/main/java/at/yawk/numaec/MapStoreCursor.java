package at.yawk.numaec;

interface MapStoreCursor extends AutoCloseable {
    long getKey();

    long getValue();

    boolean next();

    boolean elementFound();

    /**
     * Marks this cursor for possible reuse.
     */
    @Override
    void close();
}
