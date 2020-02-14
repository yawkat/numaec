package at.yawk.numaec;

public interface MapStoreCursor extends AutoCloseable {
    long getKey();

    long getValue();

    boolean next();

    boolean elementFound();

    @Override
    void close();
}
