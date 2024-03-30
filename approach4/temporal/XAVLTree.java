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
        // update no code
    }

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
    public void svrkSearchTraversal(
            Node<Date, Integer, TableRowIntDateCols> node,
            Date version,
            Integer keyStart,
            Integer keyEnd,
            List<Object> outputRows)
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
        // expand leftchild
        svrkSearchTraversal(node.leftChild, version, keyStart, keyEnd, outputRows);

        // expand rightchild
        svrkSearchTraversal(node.rightChild, version, keyStart, keyEnd, outputRows);

    }
    @Override
    public void rangeSearch1(
            Date version,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

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
        Node<Date, Integer, TableRowIntDateCols> head = avlTree.getHead();

        // start an iteration
        svrkSearchTraversal(head, version, keyStart, keyEnd, outputRows);
    }

    @Override
    public void rangeSearch2(
            Date verStart,
            Date verEnd,
            Integer key,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> outputRows)
                throws Exception {

        Node<Date, Integer, TableRowIntDateCols> head = avlTree.getHead();
        // start the traversal with the head
        mvskSearchTraversal(head, verStart,verEnd,key,outputRows);
    }

//    @Override
//    public void rangeSearch2(
//            Date verStart,
//            Date verEnd,
//            Integer key,
//            List<Object> outputRows)
//                                throws Exception {
//
//    }

    @Override
    public void rangeSearch3(
            Date verStart,
            Date verEnd,
            Integer keyStart,
            Integer keyEnd,
            ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows)
                throws Exception {

    }

    @Override
    public void rangeSearch3(
            Date verStart,
            Date verEnd,
            Integer keyStart,
            Integer keyEnd,
            List<Object> rows)
                throws Exception {

    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {

    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception {

    }

    // running the demo
    public static void main(String[] args) {

    }
}
