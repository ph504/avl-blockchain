package approach4.MerkleKDTree;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {

    public static double squareDistance(double[] pointA, double[] pointB) {
        double distance = 0.0;
        int dimensions = Math.min(pointA.length, pointB.length);
        for (int i = 0; i < dimensions; i++) {
            double diff = pointA[i] - pointB[i];
            distance += diff * diff;
        }
        return distance;
    }

    // Helper method to calculate the SHA-256 hash of a point
    public static String sha256Hash(double[] point) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            StringBuilder sb = new StringBuilder();
            for (double coordinate : point) {
                sb.append(coordinate);
            }

            byte[] hashBytes = digest.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            return getHash(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String combineAndHash(String a, String b) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest((a + b).getBytes(StandardCharsets.UTF_8));
            return getHash(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getHash(byte[] hashBytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b1 : hashBytes) {
            String hex = Integer.toHexString(0xff & b1);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
