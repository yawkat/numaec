package at.yawk.numaec;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BufferSliceTest {
    @DataProvider
    public Object[][] indices() {
        List<Object[]> out = new ArrayList<>();
        for (int start = 0; start < 6; start++) {
            for (int end = start; end < 6; end++) {
                out.add(new Object[]{ start, end - start });
            }
        }
        return out.toArray(new Object[0][]);
    }

    @Test
    public void getByte() {
        LargeByteBuffer outer = JoinedLargeByteBufferTest.fromHex("00010203040506");
        for (int start = 0; start < 6; start++) {
            for (int end = start; end < 6; end++) {
                int length = end - start;
                LargeByteBuffer slice = new BufferSlice(outer, start, length);
                for (int i = 0; i < length; i++) {
                    Assert.assertEquals(slice.getByte(i), i + start);
                }
                int finalStart = start;
                ListTest.assertThrows(IndexOutOfBoundsException.class, () -> slice.getByte(finalStart + length));
            }
        }
    }

    @Test
    public void getShort() {
        LargeByteBuffer outer = JoinedLargeByteBufferTest.fromHex("0000000100020003");
        for (int start = 0; start < 8; start += 2) {
            for (int end = start; end < 8; end += 2) {
                int length = end - start;
                LargeByteBuffer slice = new BufferSlice(outer, start, length);
                for (int i = 0; i < length; i += 2) {
                    Assert.assertEquals(slice.getShort(i), (i + start) / 2);
                }
                int finalStart = start;
                ListTest.assertThrows(IndexOutOfBoundsException.class, () -> slice.getShort(finalStart + length));
            }
        }
    }

    @Test
    public void getInt() {
        LargeByteBuffer outer = JoinedLargeByteBufferTest.fromHex("000000000000000100000002");
        for (int start = 0; start < 12; start += 4) {
            for (int end = start; end < 12; end += 4) {
                int length = end - start;
                LargeByteBuffer slice = new BufferSlice(outer, start, length);
                for (int i = 0; i < length; i += 4) {
                    Assert.assertEquals(slice.getInt(i), (i + start) / 4);
                }
                int finalStart = start;
                ListTest.assertThrows(IndexOutOfBoundsException.class, () -> slice.getInt(finalStart + length));
            }
        }
    }

    @Test
    public void getLong() {
        LargeByteBuffer outer = JoinedLargeByteBufferTest.fromHex(
                "000000000000000000000000000000010000000000000002");
        for (int start = 0; start < 24; start += 8) {
            for (int end = start; end < 24; end += 8) {
                int length = end - start;
                LargeByteBuffer slice = new BufferSlice(outer, start, length);
                for (int i = 0; i < length; i += 8) {
                    Assert.assertEquals(slice.getLong(i), (i + start) / 8);
                }
                int finalStart = start;
                ListTest.assertThrows(IndexOutOfBoundsException.class, () -> slice.getLong(finalStart + length));
            }
        }
    }
}