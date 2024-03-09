package approach4;

//import temporalSkipList.temporalSkipList.TemporalTower;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class Utils {

    public final static String HASH_DEFAULT = "SHA-256";

    public final static byte[] nullDigest = hash("0".getBytes());

    public static MessageDigest getMD() {
        return getMD(Utils.HASH_DEFAULT);
    }

    public static MessageDigest getMD(String algorithm) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return md;
    }

    public static byte[] hash(byte[] message) {
        MessageDigest md = getMD();
        md.update(message);
        return md.digest();
    }

    // TODO change use of commutative hashing to regular hashing
    public static byte[] getHash(byte[] left, byte[] right) {
        return commutativeHash(left, right);
    }

    public static byte[] commutativeHash(byte[] left, byte[] right) {
        byte[] first;
        byte[] second;

        int cmp = byteArrayCompareTo(left,right);
        if (cmp <= 0) {
            first = left;
            second = right;
        } else {
            first = right;
            second = left;
        }

        MessageDigest md = getMD();
        md.update(second);
        md.update(first);

        return md.digest(); // Note that digest() not only return a value. Calling it twice changes the output.
    }

    public static int byteArrayCompareTo(byte[] left, byte[] right) {
        int maxLen = Math.min(left.length, right.length);
        for (int i=0; i<maxLen; i++) {
            if (left[i] < right[i]) {
                return -1;
            } else if (left[i] > right[i]) {
                return 1;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    public static <K extends Comparable<K>, T extends IRowDetails<K,T,V> , V extends Comparable<V>>
    byte[] getNullableObjectHash(T obj) throws Exception {
        byte[] d = null;
        if (obj == null) {
            d = nullDigest;
        } else {
            d = obj.getDigest();
        }
        return d;
    }

    public static <T>
    byte[] getNullableObjectHash(T obj, ITypeUtils<T> typeUtils) throws Exception {
        byte[] d = null;
        if (obj == null) {
            d = nullDigest;
        } else {
            d = typeUtils.getZeroLevelDigest(obj);
        }
        return d;
    }

    public static <T extends Comparable<T>>void assertEquals(T a, T b, String msg) throws Exception {
        if ((a != null && b == null) || (a == null && b != null)) {
            throw new Exception(msg);
        } else if ((a != null && b != null) && a.compareTo(b) != 0) {
            int x =1;
            throw new Exception(msg);
        }
    }

    public static void assertTrue(boolean cond, String msg) throws Exception {
        if (!cond) {
            throw new Exception(msg);
        }
    }

    public static void assertNotNull(Object obj, String msg) throws Exception {
        if (obj == null) {
            throw new Exception(msg);
        }
    }

    public static void assertNull(Object obj, String msg) throws Exception {
        if (obj != null) {
            throw new Exception(msg);
        }
    }

    public static ArrayList<Integer> generateUniqueRandomNumbers(int init, int step, int count) {
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i=0; i<=count; i++) {
            list.add(cur);
            cur += step;
        }
        Collections.shuffle(list);
        return list;
    }



    public static <KVER extends Comparable<KVER>> void checkVersions(KVER currentVersion, KVER nextVersion) throws Exception {
        Utils.assertNotNull(nextVersion, "nextVersion can't be null");
        Utils.assertNotNull(currentVersion, "currentVersion can't be null");
        Utils.assertTrue(nextVersion.compareTo(currentVersion) >= 0, "next version must be larger or equal than current version");
    }

    public static <KVER extends Comparable<KVER>> void checkVersion(KVER version) throws Exception {
        Utils.assertNotNull(version, "version can't be null");
    }

    public static int getRandomLevel(int maxLevel, Random random, double iterationProbability) {
        int level = 0;
        while (level < maxLevel && random.nextDouble() < iterationProbability) {
            level++;
        }

        return level;
    }

    public static StringBuilder getByteArrayString(byte[] indexRootDigest) {
        StringBuilder buf = new StringBuilder();
        for (byte b : indexRootDigest) {
            buf.append(b);
            buf.append(",");
        }
        return buf;
    }

    public static void assertTrue(boolean b) throws Exception {
        assertTrue(b, "should be true");
    }

    public static void writeToFile(Path path, String s, StandardOpenOption op) {
        try {
            Files.write(path, s.getBytes(), op);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public static String stringJoin(String delimiter, Object[] array) {
        String result = "";
        if (array.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object s : array) {
                sb.append(s).append(delimiter);
            }
            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        return result;
    }

    public static <K extends Comparable<K>>
    Integer binarySearch(ArrayList<K> arr, K key) {
        int start = 0;
        int end = arr.size() - 1;

        while(start <= end) {
            int mid = start + ((end - start) / 2);

            K midRow = arr.get(mid);
            int cmp = midRow.compareTo(key);
            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }
        return null;
    }

    public static <KVER extends Comparable<KVER>> void checkVersion(KVER currentVersion, KVER nextVersion) throws Exception {
        assertTrue(nextVersion.compareTo(currentVersion) >= 0, "next version must be equal or larger than current version");
    }

    public static <K extends Comparable<K>> void checkStrictlyAscending(K smaller, K bigger) throws Exception {
        assertTrue(bigger.compareTo(smaller) > 0, "should be true");
    }





}


