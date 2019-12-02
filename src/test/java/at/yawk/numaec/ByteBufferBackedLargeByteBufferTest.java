package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ByteBufferBackedLargeByteBufferTest {
    private static ByteBuffer fromHex(String hex) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(Hex.decodeHex(hex.replaceAll("\\s", "")));
            bb.limit(bb.capacity());
            return bb;
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toString(ByteBufferBackedLargeByteBuffer bb) {
        StringBuilder builder = new StringBuilder((int) (bb.size() * 2));
        for (int i = 0; i < bb.size(); i++) {
            builder.append(Hex.encodeHex(new byte[]{ bb.getByte(i) }));
        }
        return builder.toString();
    }

    private static void assertThrows(Class<?> exception, Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            if (exception.isInstance(e)) {
                return;
            }
            throw e;
        }
        Assert.fail("Expected exception " + exception.getName());
    }

    @Test
    public void size() {
        Assert.assertEquals(
                new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ fromHex("00010203"), fromHex("0405") }, 4).size(),
                6
        );
    }

    @Test
    public void getByte() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00010203"), fromHex("0405") }, 4);
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals(bb.getByte(i), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getByte(7));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getByte(100));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getByte(-1));
    }

    @Test
    public void setByte() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00000000"), fromHex("0000") }, 4);
        for (int i = 0; i < 6; i++) {
            bb.setByte(i, (byte) i);
            Assert.assertEquals(bb.getByte(i), i);
        }
    }

    @Test
    public void getShort() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00000001"), fromHex("0002") }, 4);
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(bb.getShort(i * 2), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getShort(3 * 2));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getShort(-1 * 2));
    }

    @Test
    public void setShort() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00000000"), fromHex("0000") }, 4);
        for (int i = 0; i < 3; i++) {
            bb.setShort(i * 2, (short) i);
            Assert.assertEquals(bb.getShort(i * 2), i);
        }
    }

    @Test
    public void getInt() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("0000000000000001"), fromHex("00000002") }, 8);
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(bb.getInt(i * 4), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getInt(3 * 4));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getInt(-1 * 4));
    }

    @Test
    public void setInt() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("0000000000000000"), fromHex("00000000") }, 8);
        for (int i = 0; i < 3; i++) {
            bb.setInt(i * 4, i);
            Assert.assertEquals(bb.getInt(i * 4), i);
        }
    }

    @Test
    public void getLong() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00000000000000000000000000000001"), fromHex("0000000000000002") }, 16);
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(bb.getLong(i * 8), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getLong(3 * 8));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getLong(-1 * 8));
    }

    @Test
    public void setLong() {
        ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00000000000000000000000000000000"), fromHex("0000000000000000") }, 16);
        for (int i = 0; i < 3; i++) {
            bb.setLong(i * 8, i);
            Assert.assertEquals(bb.getLong(i * 8), i);
        }
    }

    @Test
    public void copy() {
        ByteBufferBackedLargeByteBuffer bb1 = new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00010203"), fromHex("0405") }, 4);
        for (int srcOffset = 0; srcOffset < 6; srcOffset++) {
            for (int dstOffset = 0; dstOffset < 6; dstOffset++) {
                int lengthLimit = 6 - Math.max(srcOffset, dstOffset);
                for (int length = 1; length <= lengthLimit; length++) {
                    ByteBufferBackedLargeByteBuffer bb2 = new ByteBufferBackedLargeByteBuffer(
                            new ByteBuffer[]{ fromHex("00000000"), fromHex("0000") }, 4);
                    bb2.copyFrom(bb1, srcOffset, dstOffset, length);
                    for (int i = 0; i < 6; i++) {
                        if (i < dstOffset || i >= dstOffset + length) {
                            Assert.assertEquals(bb2.getByte(i), 0);
                        } else {
                            Assert.assertEquals(bb2.getByte(i), i - dstOffset + srcOffset);
                        }
                    }
                }
                ByteBufferBackedLargeByteBuffer bb2 = new ByteBufferBackedLargeByteBuffer(
                        new ByteBuffer[]{ fromHex("00000000"), fromHex("0000") }, 4);
                boolean error = false;
                try {
                    bb2.copyFrom(bb1, srcOffset, dstOffset, lengthLimit + 1);
                } catch (IndexOutOfBoundsException e) {
                    error = true;
                }
                Assert.assertTrue(error);
            }
        }
    }

    @Test
    public void copySame() {
        for (int srcOffset = 0; srcOffset < 6; srcOffset++) {
            for (int dstOffset = 0; dstOffset < 6; dstOffset++) {
                for (int length = 1; length < 6 - Math.max(srcOffset, dstOffset); length++) {
                    ByteBufferBackedLargeByteBuffer bb = new ByteBufferBackedLargeByteBuffer(
                            new ByteBuffer[]{ fromHex("00010203"), fromHex("0405") }, 4);
                    bb.copyFrom(bb, srcOffset, dstOffset, length);
                    for (int i = 0; i < 6; i++) {
                        if (i < dstOffset || i >= dstOffset + length) {
                            Assert.assertEquals(bb.getByte(i), i);
                        } else {
                            Assert.assertEquals(bb.getByte(i), i - dstOffset + srcOffset);
                        }
                    }
                }
            }
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void weirdBufferSize3() {
        new ByteBufferBackedLargeByteBuffer(new ByteBuffer[0], 3);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void weirdBufferSize0() {
        new ByteBufferBackedLargeByteBuffer(new ByteBuffer[0], 0);
    }

    @Test
    public void copyEmptyBuffer() {
        ByteBufferBackedLargeByteBuffer bb =
                new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ fromHex("0000") }, 4);
        bb.copyFrom(LargeByteBuffer.EMPTY, 0, 0, 0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void copyDifferentBufferSize() {
        ByteBufferBackedLargeByteBuffer bb1 =
                new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ fromHex("0000") }, 4);
        ByteBufferBackedLargeByteBuffer bb2 =
                new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ fromHex("0000") }, 8);
        bb1.copyFrom(bb2, 0, 0, 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void copyNegativeLength() {
        ByteBufferBackedLargeByteBuffer bb =
                new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ fromHex("0000") }, 4);
        bb.copyFrom(LargeByteBuffer.EMPTY, 0, 0, -1);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void copyUnknown() {
        ByteBufferBackedLargeByteBuffer bb =
                new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ fromHex("0000") }, 4);
        bb.copyFrom(new LargeByteBuffer() {
            @Override
            public byte getByte(long position) throws IndexOutOfBoundsException {
                return 0;
            }

            @Override
            public short getShort(long position) throws IndexOutOfBoundsException {
                return 0;
            }

            @Override
            public int getInt(long position) throws IndexOutOfBoundsException {
                return 0;
            }

            @Override
            public long getLong(long position) throws IndexOutOfBoundsException {
                return 0;
            }

            @Override
            public void setByte(long position, byte value) throws IndexOutOfBoundsException, ReadOnlyBufferException {

            }

            @Override
            public void setShort(long position, short value) throws IndexOutOfBoundsException, ReadOnlyBufferException {

            }

            @Override
            public void setInt(long position, int value) throws IndexOutOfBoundsException, ReadOnlyBufferException {

            }

            @Override
            public void setLong(long position, long value) throws IndexOutOfBoundsException, ReadOnlyBufferException {

            }

            @Override
            public long size() {
                return 1;
            }

            @Override
            public void copyFrom(LargeByteBuffer from, long fromIndex, long toIndex, long length)
                    throws ReadOnlyBufferException, UnsupportedOperationException, IndexOutOfBoundsException {

            }
        }, 0, 0, 1);
    }
}