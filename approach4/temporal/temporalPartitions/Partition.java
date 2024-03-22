package approach4.temporal.temporalPartitions;

import approach4.IRowDetails;
import approach4.Utils;
import approach4.valueDataStructures.Version;

import java.util.Arrays;


public class Partition<K extends Comparable<K>, T extends IRowDetails<K,T,V> , V extends Comparable<V>>  {

    public final int capacity;
    private final T[] partition;
    private int size = 0;

    public Partition(int capacity) throws Exception {
        if (capacity <= 0) {
            throw new Exception("capacity should be positive");
        }

        this.capacity = capacity;
        this.partition = (T[]) new IRowDetails[capacity];
    }

    public int size() {
        return this.size;
    }

    public boolean isFull() {
        return this.size == this.capacity;
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    public void add(T newRow) throws Exception {
        if (isFull()) {
            throw new Exception("can't add. Partition is full");
        }
        this.partition[this.size++] = newRow;
    }

    //    public static <K extends Comparable<K>, T extends IRowDetails<K,T,V>, V extends Comparable<V>>
//    int getPosOfLargestElementSmallerThan(Partition<K,T,V> partition, V elemVersion) throws Exception {
//        int start = 0;
//        int end = partition.size - 1;
//
//        int prevMid = -1;
//        int mid = -1;
//        while(start <= end) {
//            prevMid = mid;
//            mid = start + ((end - start) / 2);
//
//
//            T rowDetails = partition.partition[mid];;
//            V rowVersion = rowDetails.getCreatedVersion();
//
//            if (rowVersion.compareTo(elemVersion) == 0) {
//                prevMid = mid - 1;
//                break;
//            } else if (rowVersion.compareTo(elemVersion) < 0) {
//                start = mid + 1;
//            } else {
//                end = mid - 1;
//            }
//        }
//
//        if (start > end) {
//            if (start > partition.size - 1) {
//                prevMid = partition.size - 1;
//            }
//        }
//
//        return prevMid;
//    }

//    int getPosOfLargestElementSmallerThan(V elemVersion) throws Exception {
//        return getPosOfLargestElementSmallerThan(this, elemVersion);
//    }

    public static <K extends Comparable<K>, T extends IRowDetails<K,T,V>, V extends Comparable<V>>
    int getPosEqualOrSmallerPos(V version, Partition<K, T, V> partition) throws Exception {
        int start = 0;
        int end = partition.size - 1;


        int mid = -1;
        while(start <= end) {
            mid = start + ((end - start) / 2);

            T midRow = partition.partition[mid];
            Version<V> midRowVersion = midRow.getVersion();
            V midRowValidFrom = midRowVersion.getValidFrom();

            int cmp = midRowValidFrom.compareTo(version);
            if (cmp == 0) {
                break;
            } else if (cmp < 0) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }

        int posRes;
        if (start <= end) {
            posRes = mid;
        } else {
            if (start == partition.size) {
                posRes = partition.size - 1;
            } else if (end < 0) {
                posRes = 0;
            } else {
                posRes = end;
            }
        }

        return posRes;
    }

    int getPosEqualOrSmallerPos(V version) throws Exception {
        return getPosEqualOrSmallerPos(version, this);
    }

    public T get(int pos) throws Exception {
        if (pos < 0 || pos >= this.size) {
            throw new Exception("pos should be bigger than 0 and smaller than size");
        }
        return this.partition[pos];
    }

    public int compareTo(V version) throws Exception {
        Integer res = null;
        if (this.size == 0) {
            res = -1;
        } else {

            T firstRow = this.partition[0];
            Version<V> firstRowVersion = firstRow.getVersion();
            V firstRowValidFrom = firstRowVersion.getValidFrom();

            if (this.size == 1) {
                res = firstRowValidFrom.compareTo(version);
            } else if (firstRowValidFrom.compareTo(version) == 0) {
                res = 0;
            } else if (firstRowValidFrom.compareTo(version) < 0) {
                Version<V> lastRowVersion = getLastRowVersion();
                V lastRowValidFrom = lastRowVersion.getValidFrom();

                if (lastRowValidFrom.compareTo(version) >= 0) {
                    res = 0;
                } else {
                    res = -1;
                }
            } else {
                res = 1;
            }
        }

        return res;
    }

//    public void updateLastRow(T update) throws Exception {
//        if (isEmpty()) {
//            throw new Exception("no last lastRow. Partition is empty");
//        }
//        this.partition[this.size - 1] = update;
//    }

    public T deleteLastRow() throws Exception {
        Utils.assertTrue(this.size > 0, "should be true");
        T deletedRow = getLastRow();
        this.size--;
        this.partition[this.size] = null;
        return deletedRow;
    }

    public T getLastRow() throws Exception {
        if (isEmpty()) {
            throw new Exception("no last lastRow. Partition is empty");
        }
        return this.partition[this.size - 1];
    }

    public Version<V> getLastRowVersion() throws Exception {
        if (isEmpty()) {
            throw new Exception("no last lastRow. Partition is empty");
        }
        T lastRow = this.partition[this.size - 1];
        return lastRow.getVersion();
    }

    @Override
    public String toString() {
        return Arrays.toString(partition);
    }
}

