package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static at.yawk.numaec.ListTest.assertThrows;

public class ByteBufferBackedLargeByteBufferTest {
    static ByteBuffer fromHex(String hex) {
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

    private static final int COPY_LENGTH = 10;

    @DataProvider
    public Object[][] copyIndices() {
        List<Object[]> out = new ArrayList<>();
        for (int srcOffset = 0; srcOffset < COPY_LENGTH; srcOffset++) {
            for (int dstOffset = 0; dstOffset < COPY_LENGTH; dstOffset++) {
                for (int length = 1; length < COPY_LENGTH - Math.max(srcOffset, dstOffset); length++) {
                    out.add(new Object[]{ srcOffset, dstOffset, length });
                }
            }
        }
        return out.toArray(new Object[0][]);
    }

    private static LargeByteBuffer makeSrcBuffer() {
        return new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00010203"), fromHex("04050607"), fromHex("0809") }, 4);
    }

    private static LargeByteBuffer makeZeroBuffer() {
        return new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ fromHex("00000000"), fromHex("00000000"), fromHex("0000") }, 4);
    }

    @Test(dataProvider = "copyIndices")
    public void copy(int srcOffset, int dstOffset, int length) {
        LargeByteBuffer bb1 = makeSrcBuffer();
        LargeByteBuffer bb2 = makeZeroBuffer();
        bb2.copyFrom(bb1, srcOffset, dstOffset, length);
        for (int i = 0; i < 6; i++) {
            if (i < dstOffset || i >= dstOffset + length) {
                Assert.assertEquals(bb2.getByte(i), 0);
            } else {
                Assert.assertEquals(bb2.getByte(i), i - dstOffset + srcOffset);
            }
        }
    }

    @Test(dataProvider = "copyIndices", expectedExceptions = IndexOutOfBoundsException.class)
    public void copyOutOfBounds(int srcOffset, int dstOffset, int length) {
        LargeByteBuffer bb1 = makeSrcBuffer();
        LargeByteBuffer bb2 = makeZeroBuffer();
        int lengthLimit = COPY_LENGTH - Math.max(srcOffset, dstOffset);
        bb2.copyFrom(bb1, srcOffset, dstOffset, lengthLimit + 1);
    }

    @Test(dataProvider = "copyIndices")
    public void copySame(int srcOffset, int dstOffset, int length) {
        LargeByteBuffer bb = makeSrcBuffer();
        bb.copyFrom(bb, srcOffset, dstOffset, length);
        for (int i = 0; i < COPY_LENGTH; i++) {
            if (i < dstOffset || i >= dstOffset + length) {
                Assert.assertEquals(bb.getByte(i), i);
            } else {
                Assert.assertEquals(bb.getByte(i), i - dstOffset + srcOffset);
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