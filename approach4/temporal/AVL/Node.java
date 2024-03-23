package approach4.temporal.AVL;
import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.temporal.skipList.Tower;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.temporal.temporalPartitions.Partitions;
import approach4.Utils;
import approach4.typeUtils.IntegerClassUtils;
import approach4.typeUtils.TableRowIntDateColsClassUtils;
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

    public final K key;

    public Partitions<K,V,KVER> value;

    public final int partitionCapacity;

    public Node(KVER version, V row) throws Exception {
        this(version, row, 1);
    }
    public Node(KVER version, V row, int partitionCapacity) throws Exception {

        this.key = row.getKey();

        this.partitionCapacity = partitionCapacity;
        value = new Partitions<>(this.partitionCapacity);
        if (row != null) {
            value.add(version, row);
        }

        digest = null;
        leftChild = null;
        rightChild = null;
        parent = null;
        height = 1;
    }

    private static ArrayList<Integer> generateSortedNumbers(int init, int step, int count) {
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i = 0; i< count; i++) {
            list.add(cur);
            cur += step;
        }
        return list;
    }

//    // TODO: Remove
//    private static ToweredTypeUtils<Integer, TableRowIntDateCols> getTableIntDateColsIndexTypeUtils() {
//        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
//        ITypeUtils<TableRowIntDateCols> tableRowIntDateColsClassUtils = new TableRowIntDateColsClassUtils();
//
//        return new ToweredTypeUtils<>(integerClassUtils, tableRowIntDateColsClassUtils);
//    }

    public static void main(String[] args) throws Exception{

        int patientIDsSize = 2;
        int patientIDsPerDayCount = 2;
        int datesCount = 10;
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

        ArrayList<Integer> patientIDs = generateSortedNumbers(firstPatientID, 1, patientIDsSize);

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
        List<Date> versions = versions_.stream().sorted(Comparator.comparing(o -> o)).collect(Collectors.toList());


        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        ITypeUtils<TableRowIntDateCols> tableRowIntDateColsClassUtils = new TableRowIntDateColsClassUtils();
        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = new ToweredTypeUtils<>(integerClassUtils, tableRowIntDateColsClassUtils);

        // Convert data to type data_
        List<TableRowIntDateCols> data_ = new ArrayList<>(data.size());
        for (TupleTwo<Integer, Date> row : data) {
            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            data_.add(tr);
        }


        for (TableRowIntDateCols row: data_) {
            System.out.println(row.col1 + ", " + row.col2);
            //int key = row.getKey();
            Node<Date,Integer,TableRowIntDateCols> node = new Node(row.col2, row, 1);
            System.out.println(node);
        }

    }

    @Override
    public String toString() {

        return "leftChild: " + leftChild + ", rightChild: " + rightChild + ", parent: " + parent +
                ", height: " + height + ", digest: " + Arrays.toString(digest) + ", key: " + key +
                ", value: " + value + ", partitionCapacity.: " + partitionCapacity;
    }
}
