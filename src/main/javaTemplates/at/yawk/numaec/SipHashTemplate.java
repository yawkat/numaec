/* define ClassName //
SipHash
// enddefine */
package at.yawk.numaec;

final class /*ClassName*/SipHashTemplate/**/ {
    private static final long INIT_V0 = 0x736f6d6570736575L;
    private static final long INIT_V1 = 0x646f72616e646f6dL;
    private static final long INIT_V2 = 0x6c7967656e657261L;
    private static final long INIT_V3 = 0x7465646279746573L;

    private static long rotl(long v, int shift) {
        return (v << shift) | (v >>> (64 - shift));
    }

    @SuppressWarnings({ "unused", "ConstantConditions", "UnusedAssignment" })
    private static void sipRound() {
        long v0 = 0, v1 = 0, v2 = 0, v3 = 0;
        /* define tpl_SipRound */
        v0 += v1;
        v1 = rotl(v1, 13);
        v1 ^= v0;
        v0 = rotl(v0, 32);
        v2 += v3;
        v3 = rotl(v3, 16);
        v3 ^= v2;
        v0 += v3;
        v3 = rotl(v3, 21);
        v3 ^= v0;
        v2 += v1;
        v1 = rotl(v1, 17);
        v1 ^= v2;
        v2 = rotl(v2, 32);
        /* enddefine */
        throw new AssertionError();
    }

    /**
     * cROUNDS = 2
     * dROUNDS = 4
     * inlen = 8
     * outlen = 8
     */
    @SuppressWarnings({ "StatementWithEmptyBody", "TooBroadScope" })
    public static long sipHash2_4_8_to_8(long k0, long k1, long m) {
        long v0 = INIT_V0;
        long v1 = INIT_V1;
        long v2 = INIT_V2;
        long v3 = INIT_V3;
        v3 ^= k1;
        v2 ^= k0;
        v1 ^= k1;
        v0 ^= k0;

        long b = 8L << 56;

        // round loop. only one pass because inlen=8
        v3 ^= m;
        for (int i = 0; i < 2; i++) {
            /*tpl_SipRound*/sipRound();/**/
        }
        v0 ^= m;
        // round loop end
        // message length round
        v3 ^= b;
        for (int i = 0; i < 2; i++) {
            /*tpl_SipRound*/sipRound();/**/
        }
        v0 ^= b;
        // message length round end

        // outlen == 8
        v2 ^= 0xff;

        // dround
        for (int i = 0; i < 4; i++) {
            /*tpl_SipRound*/sipRound();/**/
        }

        return v0 ^ v1 ^ v2 ^ v3;
    }
}
