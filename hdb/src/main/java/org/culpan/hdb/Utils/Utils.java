package org.culpan.hdb.Utils;

public class Utils {
    public static long bytesToLong(byte[] b, int offset) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i + offset] & 0xFF);
        }
        return result;
    }

    public static byte[] longToBytes(long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

}
