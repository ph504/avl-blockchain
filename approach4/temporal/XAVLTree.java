package approach4.temporal;

import approach4.IRowDetails;
import approach4.temporal.AVL.AVLTree;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.skipList.ToweredSkipList;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author: Arya
 * encapsulates the AVL tree and versionToKeys datastructures together
 * */
public class XAVLTree<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> implements IIndexMVIntDate {
    // the version that we have as active and is committed right now.
    private KVER currentVersion;
    // the actual AVLTree.
    final private AVLTree<KVER,K,V> avlTree;
    // the DS that contains the avl tree to map to the active keys (demoed by roaringbitmap,
    //      // roaringbitmap: which is a bitmap or a boolean array, each element corresponds to a key that is set to either 1 as active and 0 as inactive)
    // for given version range, i.e. MVAK query (multi-version on all keys)
    final private IVersionsToKeysIndex<KVER,K> versionsToKeysIndex;
    // idk, maybe the default size of bucket list.
    final private int partitionCapacity;

    /*constructor
    * @param: TOREAD toweredTypeUtils
    * @param: init version
    * @param: versionsToKeysIndex
    * @param: partitionCapacity */
    public XAVLTree(KVER initVersion, double iterationProbability, IVersionsToKeysIndex<KVER,K> versionsToKeysIndex, int partitionCapacity, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
        // set initial version
        // (meaning the first ever version inserted?
        // I guess, like version 0 (default version in the beginning)).
        this.currentVersion = initVersion;

        // idk, maybe the default size of bucket list.
        this.partitionCapacity = partitionCapacity;

        // the actual avl tree
        this.avlTree = new AVLTree<>(initVersion, iterationProbability, this.partitionCapacity, toweredTypeUtils);

        // the versions to key which is another avl tree, also see field description.
        this.versionsToKeysIndex = versionsToKeysIndex;
    }
    @Override
    public void commitCurrentVersion(Date nextVersion) throws Exception {
        //TODO
    }

    @Override
    public void insert(TableRowIntDateCols row) throws Exception {

    }

    @Override
    public void finalizeInsert() throws Exception {
        //TODO
    }

    @Override
    public void delete(Integer key) throws Exception {

    }

    @Override
    public void update(TableRowIntDateCols row) throws Exception {

    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {

    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {

    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {

    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> rows) throws Exception {

    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {

    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {

    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {

    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception {

    }
}
