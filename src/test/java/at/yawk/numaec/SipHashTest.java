package at.yawk.numaec;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.testng.Assert;
import org.testng.annotations.Test;

public final class SipHashTest {
    @Test
    public void test() {
        long k0 = 0x0706050403020100L;
        long k1 = 0x0f0e0d0c0b0a0908L;
        long m = 0x1245678954346392L;
        HashFunction hashFunction = Hashing.sipHash24(k0, k1);
        HashCode hashCode = hashFunction.hashLong(m);
        Assert.assertEquals(
                SipHash.sipHash2_4_8_to_8(k0, k1, m),
                hashCode.asLong()
        );
    }
}
