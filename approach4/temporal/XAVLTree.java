package approach4.temporal;

import approach4.IRowDetails;
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
            mvskSearchTraversal(node.rightChild, verStart, verEnd, key, outputRows);
            return;
        }

        if (node.key < key){
            mvskSearchTraversal(node.leftChild, verStart, verEnd, key, outputRows);
            return;
        }

        // We found the key.
        // find the versions
        node.value.search(verStart, verEnd, outputRows);

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
    private void mvrkSearchTraversal(
            Node<Date, Integer, TableRowIntDateCols> node,
            Date verStart,
            Date verEnd,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

        if (node==null) return;

        if (node.key > keyEnd) {
            mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);
            return;
        }
        if (node.key < keyStart) {
            mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);
            return;
        }

        // found keys within range

        // add the version range to the output list
        node.value.search(verStart, verEnd, outputRows);

        // expand other applicable keys
        mvrkSearchTraversal(node.rightChild, verStart, verEnd, keyStart, keyEnd, outputRows);

        mvrkSearchTraversal(node.leftChild, verStart, verEnd, keyStart, keyEnd, outputRows);

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
        System.err.println("why");
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
        System.err.println("why");
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

        System.err.println("why");
    }

    @Override
    public void rangeSearch4(
            Date verStart,
            Date verEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {


        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        Set<Integer> keys = versionsToKeysIndex.getKeys(verStart, verEnd);

        for(Integer key: keys){
            rangeSearch2(verStart, verEnd, key, outputRows);
        }

    }


    @Override
    public void rangeSearch4(
            Date verStart,
            Date verEnd,
            List<Object> outputRows)
                throws Exception {
        System.err.println("why");
    }

    @Override
    public String toString() {
        return "XAVLTree{" +
                "currentVersion=" + currentVersion.toString() +
                ", avlTree=" + avlTree.toString() +
                ", versionsToKeysIndex=" + versionsToKeysIndex.toString() +
                '}';
    }

    // running the demo
    public static void main(String[] args) throws Exception {
        int partitionCapacity = 1;          // partitioning the DS within the table that stores versions
        // entails how many blocks within a partition

        int patientIDsCount = 8;              // number of patients being generated
        int patientIDsPerDayCount = 8;
        int datesCount = 1;                   // number of days
        int firstPatientID = 1;                 // indexing patient IDs

        List<Date> versions = TableRowUtils.genVersions(datesCount);
        // gen keys
        ArrayList<Integer> patientIDs =
                genSortedNums(
                        firstPatientID,
                        1,
                        patientIDsCount);

//        System.out.println(patientIDs);

        ToweredTypeUtils<Integer, TableRowIntDateCols> tableRowUtils =
                TableRowUtils.getUtils();

        List<TableRowIntDateCols> data_ =
                TableRowUtils.getTableRowIntDateCols(
                        versions,
                        patientIDs,
                        patientIDsPerDayCount,
                        tableRowUtils);

        /*
         * versions
         * data_
         * tableRowUtils
         */

        Date firstVersion = versions.get(0);
        Date currentVersion = firstVersion;

        IVersionsToKeysIndex<Date,Integer> versionsToKeysIndex = new VersionsToKeysIndex(firstVersion, patientIDsCount);
        XAVLTree xtree = new XAVLTree(firstVersion, versionsToKeysIndex, partitionCapacity, tableRowUtils);

        insert(data_, currentVersion, xtree, false);
        System.out.println(xtree);


        queryTest(versions, patientIDs, xtree, 5);
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
        return keys.get(random.nextInt(keys.size()));
    }

    /**
     * Function to ensure that the end range is greater than the start range
     * @param startKey
     * @param keys
     * @return
     */
    private static int getRandomKey(int startKey, ArrayList<Integer> keys) {
        Random random = new Random();
        int endKey = random.nextInt(keys.size());
        while (endKey < startKey) {
            endKey = random.nextInt(keys.size());
        }
        return endKey;
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
     * Function to ensure that the end version is greater than the start version
     * @param versions
     * @return
     */
    private static TupleTwo<Date, Date> getRandomRangeVersions(List<Date> versions) {
        return getRandomRangeVersions(versions, 0, versions.size());
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
        System.out.println("rand ver1:"+versions.indexOf(randVer1));
        System.out.println("start range:"+startRange);
        System.out.println("end range: "+endRange);
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
        HashSet keySet = new HashSet<>();
        for (TableRowIntDateCols row : data_) {
            if(verbose) System.out.println(row.col1 + " | " + row.col2);
            // if new version immediately commit previous one
            if (!currentVersion.equals(row.col2)) {
                currentVersion = row.col2;
                xtree.commitCurrentVersion(currentVersion);
            }
            if(keySet.contains(row.getKey()))
                xtree.update(row);
            else {
                xtree.insert(row);
                keySet.add(row.getKey());
            }
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
            TupleTwo<Date, Date> randomRangeVersions = getRandomRangeVersions(versions);
            Date    randomStartVersion = randomRangeVersions.first,
                    randomEndVersion = randomRangeVersions.second;
            int randomStartKey = getRandomKey(patientIDs);
            int randomEndKey = getRandomKey(randomStartKey, patientIDs);
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows = new ArrayList<>();

//            Utils.assertTrue(randomStartKey<randomEndKey);
//            Utils.assertTrue(randomStartVersion.compareTo(randomEndVersion));
            // svrk
            System.out.println("random version: "+ randomStartVersion + " random start key: " + randomStartKey + " random end key: " + randomEndKey);
            xtree.rangeSearch1(randomStartVersion, randomStartKey, randomEndKey, outputRows);
            System.out.println("output query1.1: \n"+outputRows);

//            xtree.rangeSearch1(randomStartVersion, randomStartKey, randomStartKey, outputRows);
//            System.out.println("output query1.2:\n"+outputRows);
//
//            // mvsk
//            xtree.rangeSearch2(randomStartVersion, randomEndVersion, randomStartKey, outputRows);
//            System.out.println("output query2.1:\n"+outputRows);
//            xtree.rangeSearch2(randomStartVersion, randomStartVersion, randomStartKey, outputRows);
//            System.out.println("output query2.2:\n"+outputRows);
//
//            // mvrk
//            xtree.rangeSearch3(randomStartVersion, randomEndVersion, randomStartKey, randomEndKey, outputRows);
//            System.out.println("output query3.1:\n"+outputRows);
//            xtree.rangeSearch3(randomStartVersion, randomStartVersion, randomStartKey, randomEndKey, outputRows);
//            System.out.println("output query3.2:\n"+outputRows);
//            xtree.rangeSearch3(randomStartVersion, randomEndVersion, randomStartKey, randomStartKey, outputRows);
//            System.out.println("output query3.3:\n"+outputRows);
//            xtree.rangeSearch3(randomStartVersion, randomStartVersion, randomStartKey, randomStartKey, outputRows);
//            System.out.println("output query3.4:\n"+outputRows);
//
//            // mvak
//            xtree.rangeSearch4(randomStartVersion, randomEndVersion, outputRows);
//            System.out.println("output query4.1:\n"+outputRows);
//            xtree.rangeSearch4(randomStartVersion, randomStartVersion, outputRows);
//            System.out.println("output query4.2:\n"+outputRows);
        }
    }
    // ---------------------------------------------------------------------------
}
