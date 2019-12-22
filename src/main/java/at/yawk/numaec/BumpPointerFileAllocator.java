package at.yawk.numaec;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Set;

public final class BumpPointerFileAllocator implements LargeByteBufferAllocator, Closeable {
    private static final int MAP_SIZE_BITS = 30;

    public static final String KEEP_TEMP_FILE_PROPERTY = "at.yawk.numaec.BumpPointerFileAllocator.KEEP_TEMP_FILE";
    private static final boolean KEEP_TEMP_FILE = Boolean.getBoolean(KEEP_TEMP_FILE_PROPERTY);

    private final FileChannel channel;

    static {
        if (KEEP_TEMP_FILE) {
            System.err.println(
                    KEEP_TEMP_FILE_PROPERTY +
                    " is enabled. This is purely a debugging option - the format of file-based collections is not " +
                    "compatible across versions!");
        }
    }

    private BumpPointerFileAllocator(FileChannel channel) {
        this.channel = channel;
    }

    public static BumpPointerFileAllocator fromChannel(FileChannel channel) {
        return new BumpPointerFileAllocator(channel);
    }

    public static BumpPointerFileAllocator fromTempDirectory(Path tmpDirectory) throws IOException {
        Set<PosixFilePermission> permissions = new HashSet<>();
        permissions.add(PosixFilePermission.OWNER_READ);
        permissions.add(PosixFilePermission.OWNER_WRITE);
        Path tempFile = Files.createTempFile(
                tmpDirectory,
                BumpPointerFileAllocator.class.getName(),
                null,
                PosixFilePermissions.asFileAttribute(permissions));
        try {
            return fromChannel(FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE));
        } finally {
            // once we've opened the file, we don't need it in the file system anymore.
            if (!KEEP_TEMP_FILE) {
                Files.delete(tempFile);
            }
        }
    }

    @Override
    public LargeByteBuffer allocate(long size) {
        if (size == 0) {
            return LargeByteBuffer.EMPTY;
        }
        try {
            long start = channel.size();
            //noinspection resource
            channel.truncate(start + size);
            ByteBuffer[] parts = new ByteBuffer[(int) (((size - 1) >> MAP_SIZE_BITS) + 1)];
            for (int i = 0; i < parts.length; i++) {
                long partStart = start + ((long) i << MAP_SIZE_BITS);
                long partEnd = Math.min(start + size, start + (((long) i + 1) << MAP_SIZE_BITS));
                parts[i] = channel.map(FileChannel.MapMode.READ_WRITE, partStart, partEnd - partStart);
            }
            return new ByteBufferBackedLargeByteBuffer(parts, 1 << MAP_SIZE_BITS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
