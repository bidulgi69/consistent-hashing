package org.example;

import org.apache.commons.codec.digest.MurmurHash3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HashGenerator {

    private static MessageDigest sha256md;

    public static long generateNodeId(String nodeName) throws NoSuchAlgorithmException {
        MessageDigest md = getSha256md();
        byte[] bytes = md.digest(nodeName.getBytes(StandardCharsets.UTF_8));
        // use only 8bytes
        return ByteBuffer.wrap(bytes).getLong();
    }

    // generate a ring with range -2^31 to 2^31-1
    public static int hash(long value) {
        return MurmurHash3.hash32(value);
    }

    private static MessageDigest getSha256md() throws NoSuchAlgorithmException {
        if (sha256md == null) {
            sha256md = MessageDigest.getInstance("SHA-256");
        }

        return sha256md;
    }
}
