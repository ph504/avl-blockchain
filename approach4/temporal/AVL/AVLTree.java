package approach4.temporal.AVL;
import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.Utils;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.typeUtils.IntegerClassUtils;
import approach4.typeUtils.TableRowIntDateColsClassUtils;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

// digest=[-44, -23, -59, 84, 79, -45, -57, -108, 0, -70, 69, -110, -97, 29, 10, 9, 55, 78, 6, -120, -82, 98, -56, -80, -81, -77, 67, 108, 121, 23, 87, -15] ?

public class AVLTree<VersionType extends Comparable<VersionType>,KeyType extends Comparable<KeyType>,BucketRowType extends IRowDetails<KeyType,BucketRowType,VersionType>> {
    private final int partitionCapacity;
    private VersionType currentVersion;
    //    private final double iterationProbability;

    // TODO: Should head be final?
     private Node<VersionType,KeyType,BucketRowType> head;

    private final ToweredTypeUtils<KeyType,BucketRowType> toweredTypeUtils;


    // TODO add versioning
    // note for versioning: we don't have to add this toweredtypeutils we can call whenever we want to add it with the digest calls.
    public AVLTree(
                VersionType initVersion,
                int partitionCapacity,
                ToweredTypeUtils<KeyType, BucketRowType> toweredTypeUtils) throws Exception {
        this.currentVersion = initVersion;
        this.partitionCapacity = partitionCapacity;
        // this.head = new Node<>(initVersion, null);
        this.head = null;
        this.toweredTypeUtils = toweredTypeUtils;
    }

    public void insert(BucketRowType row) throws Exception {
        KeyType key = row.getKey();
        if (this.head == null) {
            this.head = new Node<>(this.currentVersion, row, this.partitionCapacity);
        } else {
            _insert(key, row, this.head);
        }
    }

    private void _insert(KeyType key,
                       BucketRowType row, 
                       Node<VersionType, KeyType, BucketRowType> currentNode) throws Exception{
        if (key.compareTo(currentNode.key)<0)
            if (currentNode.leftChild == null){
                System.out.println("Adding " + key + " as a left child of " + currentNode.key);
                // set as left child
                currentNode.leftChild = new Node<VersionType, KeyType, BucketRowType>(this.currentVersion, row, this.partitionCapacity);
                // set parent
                currentNode.leftChild.parent = currentNode;
                 _inspectInsertion(currentNode.leftChild, new ArrayList<Node<VersionType, KeyType, BucketRowType>>());
            }
            else _insert(key, row, currentNode.leftChild);
        
        else if (key.compareTo(currentNode.key)>0)
            if (currentNode.rightChild == null) {
                System.out.println("Adding " + key + " as a right child of " + currentNode.key);
                // set as right child
                currentNode.rightChild = new Node<VersionType, KeyType, BucketRowType>(this.currentVersion, row, this.partitionCapacity);
                // set parent
                currentNode.rightChild.parent = currentNode;
                _inspectInsertion(currentNode.rightChild, new ArrayList<Node<VersionType, KeyType, BucketRowType>>());
            }
            else _insert(key, row, currentNode.rightChild);

        else {
            // Update node
            System.out.println("Value already in tree!");
        }

    }

    private void _inspectInsertion(Node<VersionType, KeyType, BucketRowType> curNode,
                                   ArrayList<Node<VersionType, KeyType, BucketRowType>> path) throws Exception {
        if (curNode.parent == null) return;
        path.add(0, curNode); // Prepend curNode to the path

        int leftHeight = getHeight(curNode.parent.leftChild);
        int rightHeight = getHeight(curNode.parent.rightChild);

        if (Math.abs(leftHeight - rightHeight) > 1) {
            path.add(0, curNode.parent);
            _rebalanceNode(path.get(0), path.get(1), path.get(2));
            return;
        }

        // Update parent's height
        int newHeight = 1 + curNode.height;
        if (newHeight > curNode.parent.height) {
            curNode.parent.height = newHeight;
        }

        _inspectInsertion(curNode.parent, path);
    }

    private void _rebalanceNode(Node<VersionType, KeyType, BucketRowType> z,
                                Node<VersionType, KeyType, BucketRowType> y,
                                Node<VersionType, KeyType, BucketRowType> x) throws Exception {
        //  z => The unbalanced node
        //  y => The child of z
        //  x => The child of y
        if (y == z.leftChild && x == y.leftChild) {
            _rightRotate(z);
        } else if (y == z.leftChild && x == y.rightChild) {
            _leftRotate(y);
            _rightRotate(z);
        } else if (y == z.rightChild && x == y.rightChild) {
            _leftRotate(z);
        } else if (y == z.rightChild && x == y.leftChild) {
            _rightRotate(y);
            _leftRotate(z);
        } else {
            throw new Exception("z,y,x node configuration not recognized!");
        }
    }

    private void _rightRotate(Node<VersionType, KeyType, BucketRowType> z) throws Exception {
        Node<VersionType, KeyType, BucketRowType> subroot = z.parent;
        Node<VersionType, KeyType, BucketRowType> y = z.leftChild;
        Node<VersionType, KeyType, BucketRowType> t3 = y.rightChild;

        y.rightChild = z;
        z.parent = y;
        z.leftChild = t3;

        if (t3 != null) {
            t3.parent = z;
        }

        y.parent = subroot;

        if (y.parent == null) {
            this.head = y;
        } else {
            if (y.parent.leftChild == z) {
                y.parent.leftChild = y;
            } else {
                y.parent.rightChild = y;
            }
        }

        z.height = 1 + Math.max(getHeight(z.leftChild), getHeight(z.rightChild));
        y.height = 1 + Math.max(getHeight(y.leftChild), getHeight(y.rightChild));

         System.out.println("Right Rotation");
    }

    private void _leftRotate(Node<VersionType, KeyType, BucketRowType> z) throws Exception {
        Node<VersionType, KeyType, BucketRowType> subroot = z.parent;
        Node<VersionType, KeyType, BucketRowType> y = z.rightChild;
        Node<VersionType, KeyType, BucketRowType> t2 = y.leftChild;

        y.leftChild = z;
        z.parent = y;
        z.rightChild = t2;

        if (t2 != null) {
            t2.parent = z;
        }

        y.parent = subroot;

        if (y.parent == null) {
            this.head = y;
        } else {
            if (y.parent.leftChild == z) {
                y.parent.leftChild = y;
            } else {
                y.parent.rightChild = y;
            }
        }

        z.height = 1 + Math.max(getHeight(z.leftChild), getHeight(z.rightChild));
        y.height = 1 + Math.max(getHeight(y.leftChild), getHeight(y.rightChild));

         System.out.println("Left Rotation");
    }

    // Height helper
    private int getHeight(Node<VersionType, KeyType, BucketRowType> node) throws Exception {
        if (node == null) return 0;
        return node.height;
    }

    public void commitCurrentVersion(VersionType nextVersion) throws Exception {
        Utils.checkVersions(this.currentVersion, nextVersion);
        this.currentVersion = nextVersion;
        System.out.println("Current version: " + this.currentVersion);
    }

    // TODO: Remove
    private static ArrayList<Integer> generateSortedNumbers(int init, int step, int count) {
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i = 0; i< count; i++) {
            list.add(cur);
            cur += step;
        }
        return list;
    }

    // demo tests
    public static void main(String[] args) throws Exception {
        // create a tree with nodes, see if the insert and all operations works as intended
        System.out.println();

        int patientIDsSize = 6;
        int patientIDsPerDayCount = 6;
        int datesCount = 1;
        int firstPatientID = 1;

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
            // Collections.shuffle(patientIDs);
            for (int i=0; i<patientIDsPerDayCount; i++) {
                data.add(new TupleTwo<>(patientIDs.get(i), date));
            }
        }

        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        ITypeUtils<TableRowIntDateCols> tableRowIntDateColsClassUtils = new TableRowIntDateColsClassUtils();
        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = new ToweredTypeUtils<>(integerClassUtils, tableRowIntDateColsClassUtils);

        // Convert data to type data_
        List<TableRowIntDateCols> data_ = new ArrayList<>(data.size());
        for (TupleTwo<Integer, Date> row : data) {
            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            data_.add(tr);
        }

        // Get list of versions only
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            versions_.add(row.second);
        }
        List<Date> versions = versions_.stream().sorted(Comparator.comparing(o -> o)).collect(Collectors.toList());

        Date firstVersion = versions.get(0);
        Date currentVersion = firstVersion;

        AVLTree<Date, Integer, TableRowIntDateCols> index = new AVLTree(firstVersion, 1, tableIntDateColsIndexTypeUtils);

        for (TableRowIntDateCols row: data_) {
            System.out.println(row.col1 + ", " + row.col2);
        }
//
//        for (Date ver: versions) {
//            System.out.println(ver);
//        }


        for (TableRowIntDateCols row: data_) {
            System.out.println("Inserting: " + row.col1);
            if (!currentVersion.equals(row.col2)) {
                currentVersion = row.col2;
                index.commitCurrentVersion(currentVersion);
            }
            index.insert(row);
            System.out.println("-------------");
            System.out.println("Current Root: " + index.head.key);
        }
    }
}
