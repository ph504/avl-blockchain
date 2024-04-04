package approach4.temporal.AVL;
import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.temporal.temporalPartitions.Partitions;
import approach4.Utils;
import approach4.typeUtils.IntegerClassUtils;
import approach4.typeUtils.TableRowUtils;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class Node<KVER extends Comparable<KVER>,K extends Comparable<K>, V extends IRowDetails<K, V, KVER>> {
    public Node<KVER,K,V> leftChild, rightChild, parent;

    public int height;

    protected byte[] digest;

    // public final K key;
    public K key;

    public Partitions<K,V,KVER> value;

    public final int partitionCapacity;

    public Node(KVER version, V row) throws Exception {
        this(version, row, 1);
    }
    public Node(KVER version, V row, int partitionCapacity) throws Exception {
        this.key = row.getKey();

        this.partitionCapacity = partitionCapacity;
        this.value = new Partitions<>(this.partitionCapacity);
        if (row != null) {
            this.value.add(version, row);
        }

        this.digest = Utils.nullDigest;
        this.leftChild = null;
        this.rightChild = null;
        this.parent = null;
        this.height = 1;
    }

    private static ArrayList<Integer> genSortedNums(int init, int step, int count) {
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i = 0; i< count; i++) {
            list.add(cur);
            cur += step;
        }
        return list;
    }


    public void processDigest(ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
        this.digest = getNodeDigest(this, toweredTypeUtils);
        // System.out.println("Recalculated digest of node: " + this.key + ", lChild: " + this.leftChild.key + ", rChild: " + this.rightChild.key);

        StringBuilder sb = new StringBuilder("Recalculated digest of node: ");
        sb.append(this.key);

        sb.append(", lChild: ");
        if (this.leftChild != null) {
            sb.append(this.leftChild.key);
        } else {
            sb.append("null");
        }

        sb.append(", rChild: ");
        if (this.rightChild != null) {
            sb.append(this.rightChild.key);
        } else {
            sb.append("null");
        }
//        System.out.println(sb);

        for (byte b : this.digest) {
//            System.out.printf("%02X ", b);
        }
//        System.out.println();
    }

    public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K, V, KVER>>
    byte[] getKeyValueDigest(K curNodeKey, Partitions<K,V,KVER> curNodeValue, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
        // TODO: What is going on here?
        byte[] curNodeKeyDigest = Utils.getNullableObjectHash(curNodeKey, toweredTypeUtils.kTypeUtils);
        byte[] curNodeValueDigest = curNodeValue.getRootDigest();
        return Utils.getHash(curNodeKeyDigest, curNodeValueDigest);
    }

    public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V, KVER>>
    byte[] getNodeDigest(Node<KVER,K,V> node, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
        byte[] curNodeDigest = getKeyValueDigest(node.key, node.value, toweredTypeUtils);

        byte[] leftChild_digest = Utils.nullDigest;
        byte[] rightChild_digest = Utils.nullDigest;

        if (node.leftChild != null) {
            leftChild_digest = node.leftChild.digest;
        }
        if (node.rightChild != null) {
            rightChild_digest = node.rightChild.digest;
        }
        byte[] children_digest = Utils.getHash(leftChild_digest, rightChild_digest);
        return Utils.getHash(curNodeDigest, children_digest);
    }

    public static void main(String[] args) throws Exception{

        int patientIDsSize = 3;
        int patientIDsPerDayCount = 3;
        int datesCount = 1;
        int firstPatientID = 100;

        LocalDate startLocalDate = LocalDate.of(2024, Month.MARCH, 10);
        LocalDate currentLocalDate = startLocalDate;
        ZoneId defaultZoneId = ZoneId.systemDefault();
        ArrayList<Date> sequentialDates = new ArrayList<>(); // Dates starting from 2015 until datesCount goes to zero

        while (datesCount > 0) {
            Date currentDate = Date.from(currentLocalDate.atStartOfDay(defaultZoneId).toInstant());
            sequentialDates.add(currentDate);
            currentLocalDate = currentLocalDate.plusDays(1);
            datesCount--;
        }

        ArrayList<Integer> patientIDs = genSortedNums(firstPatientID, 1, patientIDsSize);

        // Generate data
        ArrayList<TupleTwo<Integer, Date>> data = new ArrayList<>();
        for (Date date : sequentialDates) {
            Collections.shuffle(patientIDs);
            for (int i=0; i<patientIDsPerDayCount; i++) {
                data.add(new TupleTwo<>(patientIDs.get(i), date));
            }
        }

        // Get list of versions only
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            versions_.add(row.second);
        }
        List<Date> versions = TableRowUtils.getColumns(data);


        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        ITypeUtils<TableRowIntDateCols> TableRowUtils = new TableRowUtils();
        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = new ToweredTypeUtils<>(integerClassUtils, TableRowUtils);
        ToweredTypeUtils<Integer, TableRowIntDateCols> toweredTypeUtils = new ToweredTypeUtils<>(integerClassUtils, TableRowUtils);


        // Convert data to type data_
        List<TableRowIntDateCols> data_ = new ArrayList<>(data.size());
        for (TupleTwo<Integer, Date> row : data) {
            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            data_.add(tr);
        }


        for (TableRowIntDateCols row: data_) {
            System.out.println(row.col1 + ", " + row.col2);
            //int key = row.getKey();
            Node<Date,Integer,TableRowIntDateCols> node = new Node<>(row.col2, row, 1);
            node.processDigest(toweredTypeUtils);
            // System.out.println(node);
        }

    }

    @Override
    public String toString() {
        return "leftChild: " + leftChild + ", rightChild: " + rightChild + ", parent: " + parent +
                ", height: " + height + ", digest: " + Arrays.toString(digest) + ", key: " + key +
                ", value: " + value + ", partitionCapacity.: " + partitionCapacity;
    }
}
