package approach4.temporal;

import approach4.CompositeKey;
import approach4.Utils;
import merkleBucketTree.MerkleTree.MerkleTree;
import merkleBucketTree.MerkleTree.Node;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MerkleBucketTreeMVCompositeIntDate {
    final public int capacity;
    final private MerkleTree<CompositeKey<Integer, Date>,Integer> merkleTree;
    private Node root;
    final private ArrayList<CompositeKey<Integer,Date>> keysVersions;


    public MerkleBucketTreeMVCompositeIntDate(int capacity) throws Exception {
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

        CompositeKey<Integer, Date> ck = new CompositeKey<>(key, version);
        this.keysVersions.add(ck);

        this.merkleTree.copyOnWriteInsert(root, ck, value);
    }

    private void getIndexValue(Date ver, Integer key, List<Integer> values) throws Exception {
        CompositeKey<Integer, Date> ck = new CompositeKey<>(key, ver);
        Integer value = merkleTree.copyOnWriteLookup(root, ck);
        Utils.assertNotNull(value, "should not be null");
        values.add(value);
    }


    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Integer> values) throws Exception {
        for (CompositeKey<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.k1;
            Date ver = keysVersions.k2;
            if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                if (ver.compareTo(version) == 0) {
                    getIndexValue(ver, key, values);
                }

            }
        }
    }

    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Integer> values) throws Exception {
        for (CompositeKey<Integer, Date> keysVersions : this.keysVersions) {
            Integer k = keysVersions.k1;
            Date version = keysVersions.k2;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (k.compareTo(key) == 0) {
                    getIndexValue(version, key, values);
                }

            }
        }
    }

    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Integer> values) throws Exception {
        for (CompositeKey<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.k1;
            Date version = keysVersions.k2;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                    getIndexValue(version, key, values);
                }
            }
        }
    }

//    public void rangeSearch4(Integer verStart, Integer verEnd, List<Integer> values) throws Exception {
//        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
//            Integer version = keysVersions.first;
//            String key = keysVersions.second;
//            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
//                getIndexValue(version, key, values);
//            }
//        }
//    }

}
