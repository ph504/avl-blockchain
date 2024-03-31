package approach4.temporal;

import approach4.IRowDetails;
import approach4.Utils;
import approach4.temporal.AVL.AVLTree;
import approach4.temporal.AVL.Node;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

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
    public XAVLTree(Date initVersion, double iterationProbability, IVersionsToKeysIndex<Date,Integer> versionsToKeysIndex, int partitionCapacity, ToweredTypeUtils<Integer,TableRowIntDateCols> toweredTypeUtils) throws Exception {
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
            svrkSearchTraversal(node.rightChild, version, keyStart, keyEnd, outputRows);
            return;
        }

        else if(node.key < keyStart) {
            svrkSearchTraversal(node.leftChild, version, keyStart, keyEnd, outputRows);
            return;
        }

        // if within the range or equal. find the version and expand further.
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

    // running the demo
    public static void main(String[] args) {

    }
}
