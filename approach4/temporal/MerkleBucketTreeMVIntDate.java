package approach4.temporal;

import approach4.TupleTwo;
import approach4.Utils;
import merkleBucketTree.MerkleTree.MerkleTree;
import merkleBucketTree.MerkleTree.Node;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MerkleBucketTreeMVIntDate {
    final public int capacity;
    final private MerkleTree<String,Integer> merkleTree;
    private Node root;
    final private ArrayList<TupleTwo<Integer, Date>> keysVersions;


    public MerkleBucketTreeMVIntDate(int capacity) throws Exception {
        this.capacity = capacity;
        this.merkleTree = new MerkleTree<>(capacity);
        this.root = null;
        this.keysVersions = new ArrayList<>();
        generateTree();
    }

    private void generateTree() throws Exception {
        ArrayList<byte[]> blocksHashes = new ArrayList<>();
        this.merkleTree.hashTable.getHashes(blocksHashes);
        this.root = this.merkleTree.generateTree(blocksHashes);
    }

    public void upsert(Date version, Integer key, Integer value) throws Exception {
        if (root == null) {
            throw new Exception("tree was not generated");
        }

        this.keysVersions.add(new TupleTwo<>(key, version));

//        DateFormat dateFormat  = new SimpleDateFormat("yyyy-mm-dd");
//        String versionStr = dateFormat.format(version);
        String compositeKey = key + "|" + version;
        this.merkleTree.copyOnWriteInsert(root, compositeKey, value);
    }

    private void getIndexValue(Date ver, Integer key, List<Object> values) throws Exception {

//        DateFormat dateFormat  = new SimpleDateFormat("yyyy-mm-dd");
//        String versionStr = dateFormat.format(ver);

        String compositeKey = key + "|" + ver;
        Integer value = merkleTree.copyOnWriteLookup(root, compositeKey);
        Utils.assertNotNull(value, "should not be null");
        values.add(value);
    }


    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Object> values) throws Exception {
        for (TupleTwo<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.first;
            Date ver = keysVersions.second;
            if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                if (ver.compareTo(version) == 0) {
                    getIndexValue(ver, key, values);
                }

            }
        }
    }

    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> values) throws Exception {
        for (TupleTwo<Integer, Date> keysVersions : this.keysVersions) {
            Integer k = keysVersions.first;
            Date version = keysVersions.second;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (k.compareTo(key) == 0) {
                    getIndexValue(version, key, values);
                }

            }
        }
    }

    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> values) throws Exception {
        for (TupleTwo<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.first;
            Date version = keysVersions.second;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                    getIndexValue(version, key, values);
                }
            }
        }
    }

    public void rangeSearch4(Date verStart, Date verEnd, List<Object> values) throws Exception {
        for (TupleTwo<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.first;
            Date version = keysVersions.second;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                getIndexValue(version, key, values);
            }
        }
    }

}
