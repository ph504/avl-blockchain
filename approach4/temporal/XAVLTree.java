package approach4.temporal;

import approach4.IRowDetails;
import approach4.MyTimer;
import approach4.TupleTwo;
import approach4.Utils;
import approach4.temporal.AVL.AVLTree;
import approach4.temporal.AVL.Node;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.VersionToKey.VersionsToKeysIndex;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.typeUtils.IntegerClassUtils;
import approach4.typeUtils.TableRowUtils;
import approach4.valueDataStructures.TableRowIntDateCols;
import org.apache.directory.mavibot.btree.Tuple;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.*;

/**
 * @author: Arya
 * encapsulates the AVL tree and ver2keys ds together
 * */
public class XAVLTree implements IIndexMVIntDate {
    // the version that we have as active and is committed right now.
    // we have this in AVL too, we have to figure out if this is indeed needed at all.
    private Date currentVersion;
    // the actual AVLTree.
    final private AVLTree<Date,Integer,TableRowIntDateCols> avlTree;
    // the DS that contains the avl tree to map to the active keys (demoed by roaringbitmap,
    //      // roaringbitmap: which is a bitmap or a boolean array, each element corresponds to a key that is set to either 1 as active and 0 as inactive)
    // for given version range, i.e. MVAK query (multi-version on all keys)
    final private IVersionsToKeysIndex<Date,Integer> versionsToKeysIndex;

    /**
    * constructor
    * @param: toweredTypeUtils
     * used solely for the int part I think that relates to digest
     * don't understand the bucket row part tho, where it's used is kinda vague and why it's there is kinda vague too.
    * @param: iteration probability:
     * the probability to build a new level
    * @param: init version
    * @param: versionsToKeysIndex
    * @param: partitionCapacity */
    public XAVLTree(
            Date initVersion,
            IVersionsToKeysIndex<Date,Integer> versionsToKeysIndex,
            int partitionCapacity,
            ToweredTypeUtils<Integer,TableRowIntDateCols> toweredTypeUtils)
                throws Exception {
        // set initial version
        // (meaning the first ever version inserted?
        // I guess, like version 0 (default version in the beginning)).
        this.currentVersion = initVersion;

        // the actual avl tree
        this.avlTree = new AVLTree<>(initVersion, partitionCapacity, toweredTypeUtils);

        // the versions to key which is another avl tree, also see field description.
        this.versionsToKeysIndex = versionsToKeysIndex;
    }

    @Override
    public void commitCurrentVersion(Date nextVersion) throws Exception {
        Utils.checkVersion(this.currentVersion, nextVersion);
        this.currentVersion = nextVersion;
        this.avlTree.commitCurrentVersion(nextVersion);
        this.versionsToKeysIndex.commit(nextVersion);
    }

    @Override
    public void insert(TableRowIntDateCols row) throws Exception {
        this.avlTree.upsert(row);
        Integer key = row.getKey();
        this.versionsToKeysIndex.add(key);
    }

    @Override
    public void finalizeInsert() throws Exception {
        // do nothing
    }

    @Override
    public void delete(Integer key) throws Exception {
        this.avlTree.delete(key);
        this.versionsToKeysIndex.deleteFromCurrentKeys(key);

    }

    @Override
    public void update(TableRowIntDateCols row) throws Exception {
        this.avlTree.upsert(row);
    }

//    private void upsert(TableRowIntDateCols row) throws Exception {
//        avlTree.upsert(row);
//        versionsToKeysIndex.add(row.getKey());
//    }


    /**
     * single version ranged key query
     * @param node
     * @param version
     * @param keyStart
     * @param keyEnd
     * @param outputRows
     * @throws Exception
     */
    public void svrkSearchTraversal(
            Node<Date, Integer, TableRowIntDateCols> node,
            Date version,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception{

        if(node==null) return;
        // if there are outside the keyrange, ignore these nodes.

        if(node.key > keyEnd) {
            svrkSearchTraversal(node.leftChild, version, keyStart, keyEnd, outputRows);
            return;
        }

        else if(node.key < keyStart) {
            svrkSearchTraversal(node.rightChild, version, keyStart, keyEnd, outputRows);
            return;
        }

        // if within the range or equal. find the version and expand further.
        // keystart <= node.key <= keyend
        IRowDetails<Integer, TableRowIntDateCols, Date> row = node.value.search(version);
        // only add if the key is relevant to the specified version.
        if (row!=null) outputRows.add(row);

        // expand rightchild
        svrkSearchTraversal(node.rightChild, version, keyStart, keyEnd, outputRows);

        // expand leftchild
        svrkSearchTraversal(node.leftChild, version, keyStart, keyEnd, outputRows);

    }

    /**
     * multi version single key query
     * @param node
     * the current node that we are exploring/expanding
     * @param verStart
     * the start version range that we are querying.
     * @param verEnd
     * the end version range that we are querying.
     * @param key
     * the key that we are trying to find
     * @param outputRows
     * the output found rows
     * @throws Exception
     */
    public void mvskSearchTraversal(
            Node<Date, Integer, TableRowIntDateCols> node,
            Date verStart,
            Date verEnd,
            Integer key,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

        if (node==null) return;

        if (node.key > key) {
            mvskSearchTraversal(node.leftChild, verStart, verEnd, key, outputRows);
            return;
        }

        if (node.key < key){
            mvskSearchTraversal(node.rightChild, verStart, verEnd, key, outputRows);
            return;
        }

        // We found the key.
        // find the versions
        node.value.search(verStart, verEnd, outputRows);

    }


    private void mvrkSearchTraversal(
            Node<Date, Integer, TableRowIntDateCols> node,
            Date verStart,
            Date verEnd,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
            throws Exception {

        if (node==null) return;

        else if (node.key > keyEnd) {
            mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);
        }
        else if (node.key < keyStart) {
            mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);
        }
        else {
            // add the version range to the output list
            node.value.search(verStart, verEnd, outputRows);

            // expand other applicable keys
//            mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);
//
//            mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);

            if (node.leftChild != null && node.key > keyStart) {
                mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);
            }
            if (node.rightChild != null && node.key < keyEnd) {
                mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);
            }
        }

    }


    /**
     *
     * @param node
     * @param verStart
     * @param verEnd
     * @param keyStart
     * @param keyEnd
     * @param outputRows
     */
//    private void mvrkSearchTraversal(
//            Node<Date, Integer, TableRowIntDateCols> node,
//            Date verStart,
//            Date verEnd,
//            Integer keyStart,
//            Integer keyEnd,
//            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
//                throws Exception {
//
//        if (node==null) return;
//
//        if (node.key > keyEnd) {
//            mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);
//            return;
//        }
//        if (node.key < keyStart) {
//            mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);
//            return;
//        }
//
//        // found keys within range
//
//        // add the version range to the output list
//        node.value.search(verStart, verEnd, outputRows);
//
//        // expand other applicable keys
//        mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);
//
//        mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);
//
//    }

//    public int optimizedSearchTraversal(Node<Date, Integer, TableRowIntDateCols> root,
//                                  List<Integer> sortedKeys,
//                                  Integer keyIndex,
//                                  Date verStart,
//                                  Date verEnd,
//                                  ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> foundRows,
//                                  Integer maxKey) throws Exception {
//
//        if (root == null) return keyIndex;
//
//        if (keyIndex >= sortedKeys.size()) return keyIndex;
//
//        Integer currentKey = sortedKeys.get(keyIndex);
//
//        // Move left if the current key is smaller than the root key
//        if (currentKey.compareTo(root.key) < 0) {
//            // System.out.println("Moving left from " + root.key + ", MaxKey: " + maxKey);
//            if (maxKey == null || root.key.compareTo(maxKey) > 0) maxKey = root.key;
//            keyIndex = optimizedSearchTraversal(root.leftChild, sortedKeys, keyIndex, verStart, verEnd, foundRows, maxKey);
//
//            if (keyIndex >= sortedKeys.size()) return keyIndex;
//            currentKey = sortedKeys.get(keyIndex);
//        }
//
//        // Process the current root if it matches the current key
//        if (currentKey.compareTo(root.key) == 0) {
//            // System.out.println("Processing node " + root.key + ", MaxKey: " + maxKey);
//            root.value.search(verStart, verEnd, foundRows);
//
//            // Move to the next key
//            keyIndex++;
//
//            if (keyIndex >= sortedKeys.size()) return keyIndex;
//            currentKey = sortedKeys.get(keyIndex);
//        }
//
//        // Move right if the current key is greater than the root key or after processing a node
//        if (currentKey.compareTo(root.key) > 0) {
//            // root.key < maxKey < currentKey
//            if (!(maxKey != null && root.key.compareTo(maxKey) < 0 && maxKey.compareTo(currentKey) <= 0)) {
//                // System.out.println("Moving right from node " + root.key + ", MaxKey: " + maxKey);
//                if (maxKey == null || root.key.compareTo(maxKey) > 0) maxKey = root.key;
//                keyIndex = optimizedSearchTraversal(root.rightChild, sortedKeys, keyIndex, verStart, verEnd, foundRows, maxKey);
//            }
//        }
//        return keyIndex;
//    }

    /**
     * Optimized traversal for searching specific keys and processing them.
     * @param root Current node in the AVL tree being examined.
     * @param sortedKeys List of keys that need to be searched in the tree.
     * @param keyIndex Current index in the list of sorted keys.
     * @param foundRows List to store the rows associated with the keys found.
     * @param maxKey Maximum key encountered on the left path to root, used to optimize right subtree traversal.
     * @return The index of the next key to be processed.
     */
    public int optimizedSearchTraversal(Node<Date, Integer, TableRowIntDateCols> root,
                                        List<Integer> sortedKeys,
                                        Integer keyIndex,
                                        Date verStart,
                                        Date verEnd,
                                        ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> foundRows,
                                        Integer minKey,
                                        Integer maxKey) throws Exception {
        if (root == null || keyIndex >= sortedKeys.size()) return keyIndex;

        Integer currentKey = sortedKeys.get(keyIndex);

        // Skip entire left subtree if no keys could be in it
        if (minKey != null && currentKey < minKey) {
            return keyIndex;
        }

        // Traverse left subtree if it may contain keys
        if (currentKey < root.key) {
//            System.out.println("Moving left from " + root.key + ", minKey: " + minKey + ", maxKey: " + maxKey);
            keyIndex = optimizedSearchTraversal(root.leftChild, sortedKeys, keyIndex, verStart, verEnd, foundRows, minKey, root.key);
            if (keyIndex >= sortedKeys.size()) return keyIndex;
            currentKey = sortedKeys.get(keyIndex);
        }

        // Process current node if it matches the current key
        if (currentKey.equals(root.key)) {
//            System.out.println("Processing " + root.key + ", minKey: " + minKey + ", maxKey: " + maxKey);
            root.value.search(verStart, verEnd, foundRows);
            keyIndex++;
            if (keyIndex >= sortedKeys.size()) return keyIndex;
            currentKey = sortedKeys.get(keyIndex);
        }

        // Skip entire right subtree if no keys could be in it
        if (maxKey != null && currentKey >= maxKey) {
            return keyIndex;
        }

        // Traverse right subtree if it may contain keys
        if (currentKey > root.key) {
//            System.out.println("Moving right from " + root.key + ", minKey: " + minKey + ", maxKey: " + maxKey);
            keyIndex = optimizedSearchTraversal(root.rightChild, sortedKeys, keyIndex, verStart, verEnd, foundRows, root.key, maxKey);
        }

        return keyIndex;
    }


    @Override
    public void rangeSearch1(
            Date version,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

        Utils.assertTrue(keyStart.compareTo(keyEnd) <= 0, "should be true");
        Node<Date, Integer, TableRowIntDateCols> head = avlTree.getHead();
        if(head==null) throw new RuntimeException("head should not be null");

        // start an iteration
        svrkSearchTraversal(head, version, keyStart, keyEnd, outputRows);
    }

    @Override
    public void rangeSearch1(
            Date version,
            Integer keyStart,
            Integer keyEnd,
            List<Object> outputRows)
                throws Exception {

    }

    @Override
    public void rangeSearch2(
            Date verStart,
            Date verEnd,
            Integer key,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        Node<Date, Integer, TableRowIntDateCols> head = avlTree.getHead();

        // start an iteration
        mvskSearchTraversal(head, verStart, verEnd, key, outputRows);

    }

    @Override
    public void rangeSearch2(
            Date verStart,
            Date verEnd,
            Integer key,
            List<Object> outputRows)
                throws Exception {
    }

    @Override
    public void rangeSearch3(
            Date verStart,
            Date verEnd,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

        Utils.assertTrue(keyStart.compareTo(keyEnd) <= 0, "should be true");
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        Node<Date, Integer, TableRowIntDateCols> head = avlTree.getHead();

        // start an iteration
        mvrkSearchTraversal(head, verStart, verEnd, keyStart, keyEnd, outputRows);
    }


    @Override
    public void rangeSearch3(
            Date verStart,
            Date verEnd,
            Integer keyStart,
            Integer keyEnd,
            List<Object> outputRows)
                throws Exception {

    }

    @Override
    public void rangeSearch4(
            Date verStart,
            Date verEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {


        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        Set<Integer> keys = versionsToKeysIndex.getKeys(verStart, verEnd);
//        System.out.println("Searchable Keys: " + keys.size());

        int keyIndex = optimizedSearchTraversal(avlTree.getHead(), new ArrayList<>(keys), 0, verStart, verEnd, outputRows, null, null);

//        for (Integer key : keys) {
//            rangeSearch2(verStart, verEnd, key, outputRows);
//        }

    }


    @Override
    public void rangeSearch4(
            Date verStart,
            Date verEnd,
            List<Object> outputRows)
                throws Exception {
    }

    @Override
    public String toString() {
        return "XAVLTree{" +
//                "currentVersion=" + currentVersion.toString() +
                ", avlTree=" + avlTree.toString() +
//                ", versionsToKeysIndex=" + versionsToKeysIndex.toString() +
                '}';
    }

    public static void main(String[] args) throws Exception {
//        int partitionCapacity = 10;          // partitioning the DS within the table that stores versions
//        // entails how many blocks within a partition
//
//        int patientIDsCount = 9;              // number of patients being generated
//        int patientIDsPerDayCount = (int)(1.0 * patientIDsCount);
//        // int patientIDsPerDayCount = 3;
//        int datesCount = 1;                   // number of days
//        int firstPatientID = 1;                 // indexing patient IDs
//
//        List<Date> versions = TableRowUtils.genVersions(datesCount);
//        // gen keys
//        ArrayList<Integer> patientIDs =
//                genSortedNums(
//                        firstPatientID,
//                        1,
//                        patientIDsCount);
//
//
//        ToweredTypeUtils<Integer, TableRowIntDateCols> tableRowUtils =
//                TableRowUtils.getUtils();
//
//        List<TableRowIntDateCols> data_ =
//                TableRowUtils.getTableRowIntDateCols(
//                        versions,
//                        patientIDs,
//                        patientIDsPerDayCount,
//                        tableRowUtils);
////        System.out.println("Total rows: " + data_.size());
////        System.out.println(versions.get(0) + " " + versions.get(versions.size() - 1));
//        /*
//         * versions
//         * data_
//         * tableRowUtils
//         */
//
//        Date firstVersion = versions.get(0);
//
//        Collections.sort(versions);
//        Collections.sort(patientIDs);
//
//        System.out.println(versions.get(0) + " " + versions.get(versions.size() - 1));
//
//        IVersionsToKeysIndex<Date,Integer> versionsToKeysIndex = new VersionsToKeysIndex<>(firstVersion, patientIDsCount);
//        XAVLTree xtree = new XAVLTree(firstVersion, versionsToKeysIndex, partitionCapacity, tableRowUtils);
//
//        insert(data_, firstVersion, xtree, true);
//
////        System.out.println(xtree);
////        System.out.println(versions);
////        System.out.println(patientIDs);
//
//        // Print the dataset
////        for (TableRowIntDateCols row: data_) {
////            System.out.println(row);
////        }
//
////        queryTest(versions, patientIDs, xtree, 1);
//
//        int delete_thresh = (int)(patientIDsCount / 2);
//        for (int i = 1; i < delete_thresh; i++) {
//            System.out.println("----------------------------\nDeleting Key: " + i);
//            xtree.delete(i);
//            System.out.println(xtree);
//        }
    }



//    utility functions
    // ---------------------------------------------------------------------------

    /**
     * Function to generate a random key from the provided list of keys
     * @param keys
     * @return
     */
    private static Integer getRandomKey(ArrayList<Integer> keys) {
        Random random = new Random();
        return keys.get(random.nextInt(keys.size()-1));
    }

    /**
     * Function to ensure that the end range is greater than the start range
     * keys has to be sorted.
     * @param startKey
     * @param keys
     * @return
     */
    private static int getRandomKey(int startKey, ArrayList<Integer> keys) {
        Random random = new Random();
        int keyStartIndex = keys.indexOf(startKey);
        int keyEndIndex = random.nextInt(keys.size()-keyStartIndex-1)+keyStartIndex+1;

        return keys.get(keyEndIndex);
    }

    /**
     * Function to generate a random version from the provided list of versions
     * @param versions
     * @return
     */
    private static Date getRandomVersion(List<Date> versions) {
        return getRandomVersion(versions, 0,versions.size());
    }
    /**
     *
     * @param versions
     * @param startRange
     * @param endRange
     * @return
     */
    private static Date getRandomVersion(List<Date> versions, int startRange, int endRange){
        Random random = new Random();
        return versions.get(random.nextInt(endRange - startRange)+startRange);
    }

    /**
     * this function requires the versions list to be sorted in ascending order.
     * @param versions
     * @param startRange
     * @param endRange
     * @return
     */
    private static TupleTwo<Date, Date> getRandomRangeVersions(List<Date> versions, int startRange, int endRange) {
        Date randVer1 = getRandomVersion(versions, startRange, endRange);
        Date randVer2 = getRandomVersion(versions, versions.indexOf(randVer1), endRange);
        TupleTwo<Date, Date> tuple = new TupleTwo<>(randVer1, randVer2);
        return tuple;
    }

    /**
     *
     * @param init
     * @param step
     * @param count
     * @return
     */
    public static ArrayList<Integer> genSortedNums(int init, int step, int count) {
        // equivalent to range list in python.
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i = 0; i< count; i++) {
            list.add(cur);
            cur += step;
        }
        return list;
    }

    // ---------------------------------------------------------------------------
    //    test functions
    // ---------------------------------------------------------------------------

    /**
     * insert
     * @param data_
     * @param currentVersion
     * @param xtree
     * @param verbose
     * @throws Exception
     */
    public static void insert(
            List<TableRowIntDateCols> data_,
            Date currentVersion,
            XAVLTree xtree,
            boolean verbose)
            throws Exception {
        // Insert the dataset
        for (TableRowIntDateCols row : data_) {
            if(verbose) System.out.println("----------------------------\nInserting: " + row.col1 + " | " + row.col2);
            // if new version immediately commit previous one and its BITMAP!
            if (!currentVersion.equals(row.col2)) {
                currentVersion = row.col2;
                xtree.commitCurrentVersion(currentVersion);
            }
//            if(keySet.contains(row.getKey()))
//                xtree.update(row);
//            else {
//                xtree.insert(row);
//                keySet.add(row.getKey());
//            }

            xtree.insert(row);
            // Print the tree
            if(verbose) System.out.println(xtree);
        }
    }

    /**
     * query
     * @param versions
     * @param patientIDs
     * @param xtree
     * @throws Exception
     */
    private static void queryTest(List<Date> versions, ArrayList<Integer> patientIDs, XAVLTree xtree, int iterationCount)
            throws Exception {
        for (int i = 0; i < iterationCount; i++) {
            TupleTwo<Date, Date> randomRangeVersions = getRandomRangeVersions(versions, 0, versions.size());
            Date    randomStartVersion = randomRangeVersions.first,
                    randomEndVersion = randomRangeVersions.second;
            int randomStartKey = getRandomKey(patientIDs);
            int randomEndKey = getRandomKey(randomStartKey, patientIDs);
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows = new ArrayList<>();

            Utils.assertTrue(randomStartKey<randomEndKey);
//            Utils.assertTrue(randomStartVersion.compareTo(randomEndVersion));

            // Single Version Range Key (SVRK)
//            System.out.println("random start version: "+ randomStartVersion + " \nrandom end version: "+ randomEndVersion + "\nrandom start key: " + randomStartKey + " \trandom end key: " + randomEndKey);
            System.out.println("Version Start: " + randomStartVersion + " | Version End: " + randomEndVersion);
            System.out.println("Start Key: " + randomStartKey + " | End Key: " + randomEndKey);

            xtree.rangeSearch1(randomStartVersion, randomStartKey, randomEndKey, outputRows);
            System.out.println("SVRK: " + outputRows);
//
            // Multi Version Single Key (MVSK)
            xtree.rangeSearch2(randomStartVersion, randomEndVersion, randomStartKey, outputRows);
            System.out.println("MVSK: "+outputRows);

            // boundary value condition check
//            xtree.rangeSearch2(randomStartVersion, randomStartVersion, randomStartKey, outputRows);
//            System.out.println("output query2.2:"+outputRows);
//
            //  Multi Value Range Key (MVRK)
            xtree.rangeSearch3(randomStartVersion, randomEndVersion, randomStartKey, randomEndKey, outputRows);
            System.out.println("MVRK:" + outputRows);
//            xtree.rangeSearch3(randomStartVersion, randomStartVersion, randomStartKey, randomEndKey, outputRows);
//            System.out.println("output query3.2:"+outputRows);
//            xtree.rangeSearch3(randomStartVersion, randomEndVersion, randomStartKey, randomStartKey, outputRows);
//            System.out.println("output query3.3:"+outputRows);
//            xtree.rangeSearch3(randomStartVersion, randomStartVersion, randomStartKey, randomStartKey, outputRows);
//            System.out.println("output query3.4:"+outputRows);

            // Multi Version All Key (MVAK)
             xtree.rangeSearch4(randomStartVersion, randomEndVersion, outputRows);
             System.out.println("MVAK: " + outputRows);
//            xtree.rangeSearch4(randomStartVersion, randomStartVersion, outputRows);
//            System.out.println("output query4.2:"+outputRows);
        }
    }
}
