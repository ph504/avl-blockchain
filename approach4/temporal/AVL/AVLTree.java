package approach4.temporal.AVL;
import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.Utils;
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

/**
 * TODO searching
 * @param <VersionType>
 * @param <KeyType>
 * @param <BucketRowType>
 */
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


    /**
     * logic: BST search,
     * if (LT) ==> leftChild
     * if (GT) ==> right Child
     * @param key
     * @param currentNode
     * return: (recursive) found Node with the key.
     * @throws Exception
     */
    public Node<VersionType, KeyType, BucketRowType> find(KeyType key,
                                                           Node<VersionType, KeyType, BucketRowType> currentNode) throws Exception {
        if (key.compareTo(currentNode.key) == 0) {
            return currentNode;
        }
        else if (key.compareTo(currentNode.key) < 0 && currentNode.leftChild != null) {
            return find(key, currentNode.leftChild);
        }
        else if (key.compareTo(currentNode.key) > 0 && currentNode.rightChild != null) {
            return find(key, currentNode.rightChild);
        }
        return null;
    }

    
    public void upsert(BucketRowType row) throws Exception {
        KeyType key = row.getKey();
        if (this.head == null) {
            this.head = new Node<>(this.currentVersion, row, this.partitionCapacity);
            this.head.processDigest(this.toweredTypeUtils);
        } else {
            upsert(key, row, this.head);
        }
    }


    private void upsert(KeyType key,
                       BucketRowType row, 
                       Node<VersionType, KeyType, BucketRowType> currentNode) throws Exception{
        if (key.compareTo(currentNode.key)<0)
            if (currentNode.leftChild == null){
                System.out.println("Inserting");
                // set as left child
                currentNode.leftChild = new Node<VersionType, KeyType, BucketRowType>(this.currentVersion, row, this.partitionCapacity);
                // set parent
                currentNode.leftChild.parent = currentNode;
                // Initialize the digest for the inserted node
                currentNode.leftChild.processDigest(this.toweredTypeUtils);
                 inspectInsertion(currentNode.leftChild, new ArrayList<Node<VersionType, KeyType, BucketRowType>>());
            }
            else upsert(key, row, currentNode.leftChild);
        
        else if (key.compareTo(currentNode.key)>0)
            if (currentNode.rightChild == null) {
                System.out.println("Inserting" );
                // set as right child
                currentNode.rightChild = new Node<VersionType, KeyType, BucketRowType>(this.currentVersion, row, this.partitionCapacity);
                // set parent
                currentNode.rightChild.parent = currentNode;
                // Initialize the digest for the inserted node
                currentNode.rightChild.processDigest(this.toweredTypeUtils);
                inspectInsertion(currentNode.rightChild, new ArrayList<Node<VersionType, KeyType, BucketRowType>>());
            }
            else upsert(key, row, currentNode.rightChild);

        else {
            System.out.println("Updating");

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

            // Update digests of its parents
            updateParentsDigests(currentNode);
        }
    }

    public BucketRowType delete(KeyType key) throws Exception {
        System.out.println("Deleting " + key);
        if (this.head != null) {
            Node<VersionType, KeyType, BucketRowType> nodeToDelete = find(key, this.head);
            if (nodeToDelete != null) {
                BucketRowType deletedRow = nodeToDelete.value.deleteLastRow(this.currentVersion);
                // Check if the bucket is empty (Because now the node needs to be deleted)
                if (nodeToDelete.value.isEmpty()) {
                    deleteNode(nodeToDelete);
                }
                else {
                    // Recompute digest of the nodeToDelete
                    nodeToDelete.processDigest(this.toweredTypeUtils);

                    // Update digests of its parents
                    updateParentsDigests(nodeToDelete);
                }
                return deletedRow;
            }
            return null;
        }
        return null;
    }

    // Update digests from nodeToDelete until the head
    private void updateParentsDigests(Node<VersionType, KeyType, BucketRowType> curNode) throws Exception {
        while (curNode.parent != null) {
            curNode.parent.processDigest(this.toweredTypeUtils);
            curNode = curNode.parent;
        }
    }

    private void deleteNode(Node<VersionType, KeyType, BucketRowType> node) throws Exception {
        // Get the parent of the node to be deleted
        Node<VersionType, KeyType, BucketRowType> nodeParent = node.parent;

        // Get the number of children of the node to be deleted
        int nodeChildren = numChildren(node);

        if (nodeChildren == 0) { // CASE 1: No children
            if (nodeParent != null) {
                if (nodeParent.leftChild == node) {
                    nodeParent.leftChild = null;
                } else {
                    nodeParent.rightChild = null;
                }
            } else {
                this.head = null;
            }
        } else if (nodeChildren == 1) { // CASE 2: One child
            // get the single child node
            Node<VersionType, KeyType, BucketRowType> child = (node.leftChild != null) ? node.leftChild : node.rightChild;

            if (nodeParent != null) {
                if (nodeParent.leftChild == node) {
                    nodeParent.leftChild = child;
                } else {
                    nodeParent.rightChild = child;
                }
            } else {
                this.head = child;
            }
            child.parent = nodeParent;

        } else { // CASE 3: Two children
            Node<VersionType, KeyType, BucketRowType> successor = _minNode(node.rightChild);

            // swapNodes(node, successor);

            node.key = successor.key;
            node.value = successor.value;

            // Where should the digest of the node digest be updated? After the recursive call of course since our
            // since the subtree will be changing!

            deleteNode(successor);

            // exit function so that we don't call the inspectDeletion twice
            return;
        }

        // Update parent's height and re-balance the tree
        if (nodeParent != null) {
            nodeParent.height = 1 + Math.max(getHeight(nodeParent.leftChild), getHeight(nodeParent.rightChild));
            inspectDeletion(nodeParent);
        }
    }

    private void inspectDeletion(Node<VersionType, KeyType, BucketRowType> curNode) throws Exception {
        if (curNode == null) {
            return;
        }

        int left_height = getHeight(curNode.leftChild);
        int right_height = getHeight(curNode.rightChild);

        if (Math.abs(left_height - right_height) > 1) {
            Node<VersionType, KeyType, BucketRowType> y = _tallerChild(curNode);
            Node<VersionType, KeyType, BucketRowType> x = _tallerChild(y);
            System.out.println("Re-balancing at trio " + curNode.key + ", " + y.key + ", " + x.key);
            Node<VersionType, KeyType, BucketRowType> rebalanced_root = _rebalanceNode(curNode, y, x);

            System.out.println("Rebalanced head at " + rebalanced_root.key);

            // Update digests from the rebalanced_root until the head, then exit the function
            while (rebalanced_root.parent != null) {
                rebalanced_root.parent.processDigest(this.toweredTypeUtils);
                rebalanced_root = rebalanced_root.parent;
            }
            return;
        }

        // # Update the parent's digest as no re-balancing was required
        curNode.processDigest(this.toweredTypeUtils);

        inspectDeletion(curNode.parent);
    }

//    private void swapNodes(Node<VersionType, KeyType, BucketRowType> node, Node<VersionType, KeyType, BucketRowType> successor) throws Exception {
//        System.out.println("Node: " + node.key + " | Successor: " + successor.key);
//        // Handle successor's parent
//        if (successor.parent != node) {
//            successor.parent.leftChild = node;
//            node.parent = successor.parent;
//        }
//        successor.parent = node.parent;
//
//        // Handle node's parent
//        if (node.parent != null) {
//            if (node.parent.leftChild == node) {
//                node.parent.leftChild = successor;
//            } else {
//                node.parent.rightChild = successor;
//            }
//        } else {
//            this.head = successor;
//        }
//
//        // Swap left children
//        successor.leftChild = node.leftChild;
//        node.leftChild = null;
//
//        if (successor.leftChild != null) {
//            successor.leftChild.parent = successor;
//        }
//
//        // Swap right children
//        if (successor == node.rightChild) {
//            node.rightChild = null;
//        }
//        else {
//            Node<VersionType, KeyType, BucketRowType> tmp = successor.rightChild;
//            successor.rightChild = node.rightChild;
//            node.rightChild = tmp;
//
//            if (node.rightChild != null) {
//                node.rightChild.parent = node;
//            }
//        }
//
//        // Swap heights
//        int tmpHeight = node.height;
//        node.height = successor.height;
//        successor.height = tmpHeight;
//
//        if (successor != node.rightChild) {
//            deleteNode(node);
//        }
//    }

    private void inspectInsertion(Node<VersionType, KeyType, BucketRowType> curNode,
                                   ArrayList<Node<VersionType, KeyType, BucketRowType>> path) throws Exception {
        if (curNode.parent == null) return;
        path.add(0, curNode); // Prepend curNode to the path

        int leftHeight = getHeight(curNode.parent.leftChild);
        int rightHeight = getHeight(curNode.parent.rightChild);

        if (Math.abs(leftHeight - rightHeight) > 1) {
            path.add(0, curNode.parent);
            System.out.println("Re-balancing at trio " + path.get(0).key + ", " + path.get(1).key + ", " + path.get(2).key);
            Node<VersionType, KeyType, BucketRowType> rebalanced_root = _rebalanceNode(path.get(0), path.get(1), path.get(2));

            // Update digests of its parents
            updateParentsDigests(rebalanced_root);
            return;
        }

        // Update parent's height
        int newHeight = 1 + curNode.height;
        if (newHeight > curNode.parent.height) {
            curNode.parent.height = newHeight;
        }

        // # Update the parent's digest as no re-balancing was required
        curNode.parent.processDigest(this.toweredTypeUtils);

        inspectInsertion(curNode.parent, path);
    }

    private Node<VersionType, KeyType, BucketRowType> _rebalanceNode(Node<VersionType, KeyType, BucketRowType> z,
                                Node<VersionType, KeyType, BucketRowType> y,
                                Node<VersionType, KeyType, BucketRowType> x) throws Exception {
        //  z => The unbalanced node
        //  y => The child of z
        //  x => The child of y
        // Returns the head of balanced subtree
        if (y == z.leftChild && x == y.leftChild) {
            rightRotate(z);
            // y is root, x is l child, z is r child
            x.processDigest(this.toweredTypeUtils);
            z.processDigest(this.toweredTypeUtils);
            y.processDigest(this.toweredTypeUtils);
            return y;
        } else if (y == z.leftChild && x == y.rightChild) {
            leftRotate(y);
            rightRotate(z);
            // x is root, z is l child, y is r child
            z.processDigest(this.toweredTypeUtils);
            y.processDigest(this.toweredTypeUtils);
            x.processDigest(this.toweredTypeUtils);
            return x;
        } else if (y == z.rightChild && x == y.rightChild) {
            leftRotate(z);
            // y is root, z is l child, x is r child
            z.processDigest(this.toweredTypeUtils);
            x.processDigest(this.toweredTypeUtils);
            y.processDigest(this.toweredTypeUtils);
            return y;
        } else if (y == z.rightChild && x == y.leftChild) {
            rightRotate(y);
            leftRotate(z);
            // x is root, y is l child, z is r child
            y.processDigest(this.toweredTypeUtils);
            z.processDigest(this.toweredTypeUtils);
            x.processDigest(this.toweredTypeUtils);
            return x;
        } else {
            throw new Exception("z,y,x node configuration not recognized!");
        }
    }

    private void rightRotate(Node<VersionType, KeyType, BucketRowType> z) throws Exception {
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
        // z.processDigest(this.toweredTypeUtils);
        // y.processDigest(this.toweredTypeUtils);
    }

    private void leftRotate(Node<VersionType, KeyType, BucketRowType> z) throws Exception {
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
        // z.processDigest(this.toweredTypeUtils);
        // y.processDigest(this.toweredTypeUtils);
    }

    private int getHeight(Node<VersionType, KeyType, BucketRowType> node) throws Exception {
        if (node == null) return 0;
        return node.height;
    }

    private int numChildren(Node<VersionType, KeyType, BucketRowType> node) throws Exception {
        int count = 0;
        if (node.leftChild != null) count++;
        if (node.rightChild != null) count++;
        return count;
    }

    private Node<VersionType, KeyType, BucketRowType> _tallerChild(Node<VersionType, KeyType, BucketRowType> curNode) throws Exception {
        int left = getHeight(curNode.leftChild);
        int right = getHeight(curNode.rightChild);
        if (left >= right) {
            return curNode.leftChild;
        }
        else {
            return curNode.rightChild;
        }
    }

    private Node<VersionType, KeyType, BucketRowType> _minNode(Node<VersionType, KeyType, BucketRowType> node) throws Exception {
        Node<VersionType, KeyType, BucketRowType> current = node;
        while (current.leftChild != null) {
            current = current.leftChild;
        }
        return current;
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
    public static ArrayList<Integer> generateSortedNumbers(int init, int step, int count) {
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
        int patientIDsSize = 8;
        int patientIDsPerDayCount = 8;
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

        // Print the synthetic dataset
        for (TableRowIntDateCols row: data_) {
            System.out.println(row.col1 + ", " + row.col2);
        }

        // Insert the dataset
        for (TableRowIntDateCols row: data_) {
            System.out.println(row.col1 + " | " + row.col2);
            if (!currentVersion.equals(row.col2)) {
                currentVersion = row.col2;
                index.commitCurrentVersion(currentVersion);
            }
            index.upsert(row);

            // Print the tree
            System.out.println(index);

        }

        // Delete
        Random random = new Random();
        TableRowIntDateCols del_row = index.delete(random.nextInt(patientIDsSize) + 1);
        System.out.println(del_row);
        System.out.println(index);


        // Randomly update 20% of the shuffled rows
//        int totalRowsToUpdate = (int) (data_.size() * 0.2);
//        System.out.println("Total rows to update: " + totalRowsToUpdate);
//        Collections.shuffle(data_);
//
//        for (int i = 0; i < totalRowsToUpdate; i++) {
//            TableRowIntDateCols row = data_.get(i);
//            // Update the version to a random date (in this case, a fixed sample date for demonstration)
//            LocalDate tmp = LocalDate.of(2023, Month.FEBRUARY, 28);
//            Date sampleDate = Date.from(tmp.atStartOfDay(defaultZoneId).toInstant());
//            TableRowIntDateCols updateRow = new TableRowIntDateCols(row.col1, sampleDate, tableIntDateColsIndexTypeUtils.vTypeUtils);
//
//            System.out.println("Updating: " + updateRow.col1 + " | " + updateRow.col2);
//            index.update_insert(updateRow);
//        }
    }
}
