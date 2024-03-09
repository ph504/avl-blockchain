package approach4.temporal.temporalPartitions;

import approach4.IAuthDigest;
import approach4.IRowDetails;
import approach4.Utils;

import java.util.ArrayList;

public class PartitionSearchRes<K extends Comparable<K>, T extends IRowDetails<K,T,V>, V extends Comparable<V>> implements IAuthDigest {
    public final byte[] prevRowDigest;
    public final ArrayList<T> rows;

    public PartitionSearchRes(byte[] prevRowDigest, ArrayList<T> rows) throws Exception {
//        Utils.assertNotNull(prevRowDigest, "should not be null");
//        Utils.assertTrue(rows != null && rows.size() > 0, "should be true");
        this.prevRowDigest = prevRowDigest;
        this.rows = rows;
    }

    public T validateAndGetResults(V searchKey, byte[] rootDigest) throws Exception {
        byte[] rollingDigest = getDigest();
        if (Utils.byteArrayCompareTo(rollingDigest, rootDigest) != 0) {
            throw new Exception("proof doesn't match on root digest!");
        }

        return validateAndGetResults(searchKey);
    }

    public T validateAndGetResults(V searchKey) throws Exception {
        T foundRow = this.rows.get(0);
        if (!Partitions.isRowMatchRowVersion(foundRow,searchKey)) {
            throw new Exception("validation failed");
        }
        return foundRow;
    }

//    public T validateAndGetResults(V searchKey) throws Exception {
//        T foundRow = null;
//        int rowsCount = this.rows.size();
//
//        foundRow = this.prevRow;
//
//        if (rowsCount > 0) {
//            int rowPos = 0;
//            int stateMachine = 0; // 0 - init, 1 - value bigger than searchKey, 2 - value smaller than search key,
//            // 3 - version found and next value should be bigger than search key
//            while (rowPos < rowsCount) {
//                T row = this.rows.get(rowPos);
//                V rowVersion = row.getVersion();
//                if (rowVersion.compareTo(searchKey) < 0) {
//                    foundRow = row;
//                    if (stateMachine == 0) {
//                        stateMachine = 2;
//                    } else if (stateMachine == 1) {
//                        throw new Exception("authentication failed");
//                    }
//                    if (stateMachine == 2) {
//                        // do nothing
//                    } else {
//                        throw new Exception("authentication failed");
//                    }
//                } else if (rowVersion.compareTo(searchKey) > 0) {
//                    if (stateMachine == 0) {
//                        stateMachine = 1;
//                    } else if (stateMachine == 1) {
//                        // do nothing
//                    }
//                    if (stateMachine == 2) {
//                        throw new Exception("authentication failed");
//                    } else {
//                        // do nothing
//                    }
//                } else {
//                    foundRow = row;
//                    if (stateMachine == 0) {
//                        stateMachine = 3;
//                    } else {
//                        throw new Exception("authentication failed");
//                    }
//                }
//
//                rowPos++;
//            }
//        }
//
//        return foundRow;
//    }

    public byte[] getDigest() throws Exception {
        byte[] rollingDigest = this.prevRowDigest;
        for (T r : this.rows) {
            rollingDigest = r.calculateDigest(rollingDigest);
        }
        return rollingDigest;
    }

//    public T getLastItem() {
//        T res = this.rows.get(0);
//        return res;
//    }
}
