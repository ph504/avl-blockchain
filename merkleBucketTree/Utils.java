package merkleBucketTree;

//import temporalSkipList.temporalSkipList.TemporalTower;

import approach4.CompositeKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Utils {

//    public final static String HASH_DEFAULT = "SHA-256";
    public final static String HASH_DEFAULT = "SHA-256";

    // Used for computing hashes of sentinel nodes
//    public final static String ZERO = "0";
    public final static byte[] nullDigest = hash("0".getBytes());

//    public static byte[] getDigest(Object obj) throws Exception {
//        byte[] digest;
//        byte[] ba;
//        if (obj instanceof Integer) {
//            ba = ByteBuffer.allocate(4).putInt((Integer) obj).array();
//            digest = hash(ba);
//        } else if (obj instanceof String) {
//            ba = ByteBuffer.wrap(((String) obj).getBytes()).array();
//            digest = hash(ba);
//        } else if (obj instanceof IDigest) {
//            digest = ((IDigest) obj).getZeroLevelDigest();
//        } else {
//            throw new Exception("unsupported object type");
//        }
//
//        return digest;
//    }

//    public static byte[] getNullDigest() {
//        byte[] ba = ZERO.getBytes();
//        return hash(ba);
//    }



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
        int maxLen = Math.max(left.length, right.length);
        for (int i=0; i<maxLen; i++) {
            if (left[i] < right[i]) {
                return -1;
            } else if (left[i] > right[i]) {
                return 1;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    public static String hexFromBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "Parameter `bytes` cannot be null.");
        StringBuilder hexString = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            appendByteAsHexPair(b, hexString);
        }
        return hexString.toString();
    }

    private static void appendByteAsHexPair(byte b, StringBuilder sb) {
        assert sb != null;
        byte leastSignificantHalf = (byte) (b & 0x0f);
        byte mostSignificantHalf = (byte) ((b >> 4) & 0x0f);
        sb.append(getHexDigitWithValue(mostSignificantHalf));
        sb.append(getHexDigitWithValue(leastSignificantHalf));
    }

    private static char getHexDigitWithValue(byte value) {
        assert value >= 0 && value <= 16;
        if (value < 10) {
            return (char) ('0' + value);
        }
        return (char) ('A' + value - 10);
    }



    public static byte[] getNullableDigest(Object obj) throws Exception {
        byte[] digest = null;
        if (obj == null) {
            digest = approach4.Utils.nullDigest;
        } else if (obj instanceof String) {
            String o = (String) obj;
            digest = o.getBytes();
            digest = approach4.Utils.hash(digest);
        } else if (obj instanceof Integer) {
            Integer o = (Integer) obj;
            digest = ByteBuffer.allocate(4).putInt(o).array();
            digest = approach4.Utils.hash(digest);
        } else if (obj instanceof byte[]) {
            byte[] o = (byte[]) obj;
            digest = approach4.Utils.hash(o);
        } else if (obj instanceof CompositeKey) {
            ByteBuffer bb = ByteBuffer.allocate(24+4);
            bb.putLong(0, ((CompositeKey<Integer, Date>) obj).k2.getTime());
            bb.putInt(24, ((CompositeKey<Integer, Date>) obj).k1);
            byte[] o = bb.array();
            digest = approach4.Utils.hash(o);
        }
        else {
            throw new Exception("type of key not supported");
        }
        return digest;
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

//    public static <K extends Comparable<K>,V>
//    byte[] getNullableDigest(IToweredZeroLevelAuthDigest<K,V> toweredZeroLevelAuthDigest) throws Exception {
//        byte[] digest = null;
//        if (toweredZeroLevelAuthDigest == null) {
//            digest = Utils.getNullDigest();
//        } else {
//            digest = toweredZeroLevelAuthDigest.getDigest();
//        }
//        return digest;
//    }



//    public static void printTemporalSkipList(TemporalToweredSkipList<Integer, Integer, Integer> tsl) throws Exception {
//        ArrayList<TemporalTower<Integer, Integer, Integer>> temporalSortedItems = tsl.getSortedItems(0);
//        for (int k = 0, temporalSortedItemsSize = temporalSortedItems.size(); k < temporalSortedItemsSize; k++) {
//            TemporalTower<Integer, Integer, Integer> tt = temporalSortedItems.get(k);
//            System.out.println("TemporalTower: " + tt);
//
//            ArrayList<ToweredSkipList<Integer, TemporalTower<Integer, Integer, Integer>>> levelNodes = tt.levelNodes;
//            for (int i = levelNodes.size() - 1; i >= 0; i--) {
//                ToweredSkipList<Integer, TemporalTower<Integer, Integer, Integer>> index = levelNodes.get(i);
//                System.out.println("-->index level: " + i);
//                if (index != null) {
//                    ArrayList<Tower<Integer, TemporalTower<Integer, Integer, Integer>>> sortedItems = index.getSortedItems();
//                    for (int j = sortedItems.size() - 1; j >= 0; j--) {
//                        Tower<Integer, TemporalTower<Integer, Integer, Integer>> t = sortedItems.get(j);
//                        System.out.println("---->(tower level: " + j + ") " + t);
//                    }
//                }
//            }
//        }
//        System.out.println("-------------------------------");
//    }

//    public static Method clone;
//    static {
//        try {
//            clone = Object.class.getMethod("clone");
//        } catch (NoSuchMethodException e) {
//            e.printStackTrace();
//        }
//    }
//
//    static public <V>
//    V clone(V obj) throws InvocationTargetException, IllegalAccessException {
//        return (V) clone.invoke(obj);
//    }

}


