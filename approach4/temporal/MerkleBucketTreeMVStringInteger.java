package approach4.temporal;

import approach4.TupleTwo;
import approach4.Utils;
import merkleBucketTree.MerkleTree.MerkleTree;
import merkleBucketTree.MerkleTree.Node;

import java.util.ArrayList;
import java.util.List;

public class MerkleBucketTreeMVStringInteger {
    final public int capacity;
    final private MerkleTree<String,Integer> merkleTree;
    private Node root;
    final private ArrayList<TupleTwo<Integer, String>> keysVersions;


    public MerkleBucketTreeMVStringInteger(int capacity) throws Exception {
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

    public void upsert(Integer version, String key, Integer value) throws Exception {
        if (root == null) {
            throw new Exception("tree was not generated");
        }

        this.keysVersions.add(new TupleTwo<>(version, key));
        String compositeKey = key + "|" + version;
        this.merkleTree.copyOnWriteInsert(root, compositeKey, value);
    }

    private void getIndexValue(Integer ver, String key, List<Integer> values) throws Exception {
        String compositeKey = key + "|" + ver;
        Integer value = merkleTree.copyOnWriteLookup(root, compositeKey);
        Utils.assertNotNull(value, "should not be null");
        values.add(value);
    }


    public void rangeSearch1(Integer version, String keyStart, String keyEnd, List<Integer> values) throws Exception {
        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
            Integer ver = keysVersions.first;
            String key = keysVersions.second;
            if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                if (ver.compareTo(version) == 0) {
                    getIndexValue(ver, key, values);
                }

            }
        }
    }

    public void rangeSearch2(Integer verStart, Integer verEnd, String key, List<Integer> values) throws Exception {
        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
            Integer version = keysVersions.first;
            String k = keysVersions.second;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (k.compareTo(key) == 0) {
                    getIndexValue(version, key, values);
                }

            }
        }
    }

    public void rangeSearch3(Integer verStart, Integer verEnd, String keyStart, String keyEnd, List<Integer> values) throws Exception {
        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
            Integer version = keysVersions.first;
            String key = keysVersions.second;
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
