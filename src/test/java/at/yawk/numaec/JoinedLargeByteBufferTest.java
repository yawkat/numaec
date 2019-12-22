package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static at.yawk.numaec.ListTest.assertThrows;

public class JoinedLargeByteBufferTest {
    static LargeByteBuffer fromHex(String hex) {
        return new ByteBufferBackedLargeByteBuffer(
                new ByteBuffer[]{ ByteBufferBackedLargeByteBufferTest.fromHex(hex) }, 0x100000);
    }

    @Test
    public void size() {
        Assert.assertEquals(
                new JoinedBufferImpl(fromHex("00010203"), fromHex("0405")).size(),
                6
        );
    }

    @Test
    public void getByte() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("00010203"), fromHex("0405"));
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals(bb.getByte(i), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getByte(7));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getByte(100));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getByte(-1));
    }

    @Test
    public void setByte() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("00000000"), fromHex("0000"));
        for (int i = 0; i < 6; i++) {
            bb.setByte(i, (byte) i);
            Assert.assertEquals(bb.getByte(i), i);
        }
    }

    @Test
    public void getShort() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("00000001"), fromHex("0002"));
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(bb.getShort(i * 2), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getShort(3 * 2));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getShort(-1 * 2));
    }

    @Test
    public void setShort() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("00000000"), fromHex("0000"));
        for (int i = 0; i < 3; i++) {
            bb.setShort(i * 2, (short) i);
            Assert.assertEquals(bb.getShort(i * 2), i);
        }
    }

    @Test
    public void getInt() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("0000000000000001"), fromHex("00000002"));
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(bb.getInt(i * 4), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getInt(3 * 4));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getInt(-1 * 4));
    }

    @Test
    public void setInt() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("0000000000000000"), fromHex("00000000"));
        for (int i = 0; i < 3; i++) {
            bb.setInt(i * 4, i);
            Assert.assertEquals(bb.getInt(i * 4), i);
        }
    }

    @Test
    public void getLong() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("00000000000000000000000000000001"), fromHex("0000000000000002"));
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(bb.getLong(i * 8), i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getLong(3 * 8));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getLong(-1 * 8));
    }

    @Test
    public void setLong() {
        JoinedBufferImpl bb = new JoinedBufferImpl(
                fromHex("00000000000000000000000000000000"), fromHex("0000000000000000"));
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
        return new JoinedBufferImpl(
                fromHex("00010203"), fromHex("04050607"), fromHex("0809"));
    }

    private static LargeByteBuffer makeZeroBuffer() {
        return new JoinedBufferImpl(
                fromHex("00000000"), fromHex("00000000"), fromHex("0000"));
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

    private static class JoinedBufferImpl extends JoinedLargeByteBuffer {
        private final List<LargeByteBuffer> components;

        JoinedBufferImpl(LargeByteBuffer... components) {
            this.components = Arrays.asList(components);
        }

        @Override
        protected LargeByteBuffer component(long position) {
            for (LargeByteBuffer component : components) {
                if (position < component.size()) {
                    return component;
                }
                position -= component.size();
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        protected long offset(long position) {
            for (LargeByteBuffer component : components) {
                if (position < component.size()) {
                    return position;
                }
                position -= component.size();
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        protected long nextRegionStart(long position) {
            long remaining = position;
            for (LargeByteBuffer component : components) {
                if (remaining < 0) {
                    break;
                }
                remaining -= component.size();
            }
            if (remaining < 0) {
                return position - remaining;
            } else {
                throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public long size() {
            return components.stream().mapToLong(LargeByteBuffer::size).sum();
        }
    }
}