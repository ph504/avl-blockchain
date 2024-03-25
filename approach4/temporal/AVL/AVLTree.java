package approach4.temporal.AVL;
import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.Utils;
import approach4.temporal.skipList.Tower;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.typeUtils.IntegerClassUtils;
import approach4.typeUtils.TableRowIntDateColsClassUtils;
import approach4.valueDataStructures.TableRowIntDateCols;
import approach4.valueDataStructures.Version;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class AVLTree<VersionType extends Comparable<VersionType>,KeyType extends Comparable<KeyType>,BucketRowType extends IRowDetails<KeyType,BucketRowType,VersionType>> {
    private final int partitionCapacity;
    private VersionType currentVersion;
    //    private final double iterationProbability;

     private Node<VersionType,KeyType,BucketRowType> head;

    private final ToweredTypeUtils<KeyType,BucketRowType> toweredTypeUtils;


    // TODO add versioning
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

    public void update_insert(BucketRowType row) throws Exception {
        KeyType key = row.getKey();
        if (this.head == null) {
            this.head = new Node<>(this.currentVersion, row, this.partitionCapacity);
            this.head.processDigest(this.toweredTypeUtils);
        } else {
            _update_insert(key, row, this.head);
        }
    }


    private void _update_insert(KeyType key,
                       BucketRowType row, 
                       Node<VersionType, KeyType, BucketRowType> currentNode) throws Exception{
        if (key.compareTo(currentNode.key)<0)
            if (currentNode.leftChild == null){
                // set as left child
                currentNode.leftChild = new Node<VersionType, KeyType, BucketRowType>(this.currentVersion, row, this.partitionCapacity);
                // set parent
                currentNode.leftChild.parent = currentNode;
                // Initialize the digest for the inserted node
                currentNode.leftChild.processDigest(this.toweredTypeUtils);
                 _inspectInsertion(currentNode.leftChild, new ArrayList<Node<VersionType, KeyType, BucketRowType>>());
            }
            else _update_insert(key, row, currentNode.leftChild);
        
        else if (key.compareTo(currentNode.key)>0)
            if (currentNode.rightChild == null) {
                // set as right child
                currentNode.rightChild = new Node<VersionType, KeyType, BucketRowType>(this.currentVersion, row, this.partitionCapacity);
                // set parent
                currentNode.rightChild.parent = currentNode;
                // Initialize the digest for the inserted node
                currentNode.rightChild.processDigest(this.toweredTypeUtils);
                _inspectInsertion(currentNode.rightChild, new ArrayList<Node<VersionType, KeyType, BucketRowType>>());
            }
            else _update_insert(key, row, currentNode.rightChild);

        else {
            BucketRowType lastRow = currentNode.value.getLastRow();
            Version<VersionType> lastRowVersion = lastRow.getVersion();
            if (lastRowVersion.getValidTo() == null) {
            //  throw new Exception("a row with the same key already exists");
                BucketRowType updatedRow = currentNode.value.updateLastRow(this.currentVersion, row);
            } else {
                Utils.assertTrue(lastRowVersion.getValidFrom().compareTo(this.currentVersion)  < 0);
                BucketRowType addedRow = currentNode.value.add(this.currentVersion, row);
            }

            // Update digest
            currentNode.processDigest(this.toweredTypeUtils);

            // Update digests from the current node to root up
            while (currentNode.parent != null) {
                currentNode.parent.processDigest(this.toweredTypeUtils);
                currentNode = currentNode.parent;
            }
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
            System.out.println("Rebalancing starting at node " + curNode.parent.key);
            Node<VersionType, KeyType, BucketRowType> rebalanced_root = _rebalanceNode(path.get(0), path.get(1), path.get(2));

            System.out.println("Rebalanced root at " + rebalanced_root.key);

            // Update digests from the rebalanced root up, then exit the function
            while (rebalanced_root.parent != null) {
                rebalanced_root.parent.processDigest(this.toweredTypeUtils);
                rebalanced_root = rebalanced_root.parent;
            }
            return;
        }

        // Update parent's height
        int newHeight = 1 + curNode.height;
        if (newHeight > curNode.parent.height) {
            curNode.parent.height = newHeight;
        }

        // # Update the parent's digest as no re-balancing was required
        System.out.println("Updating my parent's digest");
        curNode.parent.processDigest(this.toweredTypeUtils);

        _inspectInsertion(curNode.parent, path);
    }

    private Node<VersionType, KeyType, BucketRowType> _rebalanceNode(Node<VersionType, KeyType, BucketRowType> z,
                                Node<VersionType, KeyType, BucketRowType> y,
                                Node<VersionType, KeyType, BucketRowType> x) throws Exception {
        //  z => The unbalanced node
        //  y => The child of z
        //  x => The child of y
        // Returns the root of balanced subtree
        if (y == z.leftChild && x == y.leftChild) {
            _rightRotate(z);
            return y;
        } else if (y == z.leftChild && x == y.rightChild) {
            _leftRotate(y);
            _rightRotate(z);
            return x;
        } else if (y == z.rightChild && x == y.rightChild) {
            _leftRotate(z);
            return y;
        } else if (y == z.rightChild && x == y.leftChild) {
            _rightRotate(y);
            _leftRotate(z);
            return x;
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

        // Update the digests of z and y (Since z is the child of y, we first update z)
        z.processDigest(this.toweredTypeUtils);
        y.processDigest(this.toweredTypeUtils);
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

        // Update the digests of z and y (Since z is the child of y, we first update z)
        z.processDigest(this.toweredTypeUtils);
        y.processDigest(this.toweredTypeUtils);
    }

    private int getHeight(Node<VersionType, KeyType, BucketRowType> node) throws Exception {
        if (node == null) return 0;
        return node.height;
    }

    public void commitCurrentVersion(VersionType nextVersion) throws Exception {
        Utils.checkVersions(this.currentVersion, nextVersion);
        this.currentVersion = nextVersion;
        System.out.println("Current version: " + this.currentVersion);
    }

    @Override
    public String toString() {
        if (this.head == null) {
            return "";
        }

        StringBuilder content = new StringBuilder("\n"); // To hold the final string
        List<Node<VersionType, KeyType, BucketRowType>> curNodes = new ArrayList<>(); // All nodes at the current level
        curNodes.add(this.head);
        int curHeight = this.head.height; // Height of nodes at the current level
        String sep = " ".repeat(Math.max(0, (int) Math.pow(2, curHeight - 1))); // Variable sized separator between elements

        while (true) {
            curHeight--;
            if (curNodes.isEmpty()) {
                break;
            }

            StringBuilder curRow = new StringBuilder(" ");
            StringBuilder nextRow = new StringBuilder("");
            List<Node<VersionType, KeyType, BucketRowType>> nextNodes = new ArrayList<>();

            boolean allNulls = curNodes.stream().allMatch(Objects::isNull);
            if (allNulls) {
                break;
            }

            for (Node<VersionType, KeyType, BucketRowType> n : curNodes) {
                if (n == null) {
                    curRow.append("   ").append(sep);
                    nextRow.append("   ").append(sep);
                    nextNodes.add(null);
                    nextNodes.add(null);
                    continue;
                }

                if (n.key != null) {
                    String buf = " ".repeat(Math.max(0, (5 - String.valueOf(n.key).length()) / 2));
                    curRow.append(buf).append(n.key).append(buf).append(sep);
                } else {
                    curRow.append(" ".repeat(5)).append(sep);
                }

                if (n.leftChild != null) {
                    nextNodes.add(n.leftChild);
                    nextRow.append(" /").append(sep);
                } else {
                    nextRow.append("  ").append(sep);
                    nextNodes.add(null);
                }

                if (n.rightChild != null) {
                    nextNodes.add(n.rightChild);
                    nextRow.append("\\ ").append(sep);
                } else {
                    nextRow.append("  ").append(sep);
                    nextNodes.add(null);
                }
            }

            content.append(" ".repeat(Math.max(0, curHeight * 3)))
                    .append(curRow)
                    .append("\n")
                    .append(" ".repeat(Math.max(0, curHeight * 3)))
                    .append(nextRow)
                    .append("\n");
            curNodes = nextNodes;
            sep = " ".repeat(Math.max(0, sep.length() / 2)); // Cut separator size in half
        }
        return content.toString();
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
        int patientIDsSize = 7;
        int patientIDsPerDayCount = 7;
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
              Collections.shuffle(patientIDs);
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

        AVLTree<Date, Integer, TableRowIntDateCols> index = new AVLTree<>(firstVersion, 1, tableIntDateColsIndexTypeUtils);

        for (TableRowIntDateCols row: data_) {
            System.out.println(row.col1 + ", " + row.col2);
        }

        for (TableRowIntDateCols row: data_) {
            System.out.println("Inserting: " + row.col1 + " | " + row.col2);
            if (!currentVersion.equals(row.col2)) {
                currentVersion = row.col2;
                index.commitCurrentVersion(currentVersion);
            }
            index.update_insert(row);

            // Print the tree
            System.out.println(index);

        }


        int totalRowsToUpdate = (int) (data_.size() * 0.2);
        System.out.println("Total rows to update: " + totalRowsToUpdate);
        Collections.shuffle(data_);

        // Update only the first 20% of the shuffled rows
        for (int i = 0; i < totalRowsToUpdate; i++) {
            TableRowIntDateCols row = data_.get(i);
            // Update the version to a random date (in this case, a fixed sample date for demonstration)
            LocalDate tmp = LocalDate.of(2023, Month.FEBRUARY, 28);
            Date sampleDate = Date.from(tmp.atStartOfDay(defaultZoneId).toInstant());
            TableRowIntDateCols updateRow = new TableRowIntDateCols(row.col1, sampleDate, tableIntDateColsIndexTypeUtils.vTypeUtils);

            System.out.println("Updating: " + updateRow.col1 + " | " + updateRow.col2);
            index.update_insert(updateRow);
        }
    }
}
