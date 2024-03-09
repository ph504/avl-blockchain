package approach4.temporal.VersionToKey;

import approach4.AVL.AVLTree;
import approach4.Utils;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public class VersionsToKeysIndex<KVER extends Comparable<KVER>, K extends Comparable<K>> implements IVersionsToKeysIndex<KVER, K> {
    private KVER currentVersion;
//    final private GenericBST<KVER, RoaringBitmap> versionsToBitmap;
    final private AVLTree<KVER,RoaringBitmap> versionsToBitmap;
    private RoaringBitmap uncommittedBitmap;
//    final private ArrayList<Integer> myKeysToPos;

    final private HashMap<K,Integer> keysToBitmapPositions;
    final private HashMap<Integer,K> bitmapPositionsToKeys;

    public VersionsToKeysIndex(KVER initVersion, int keysCapacity) {
        this.uncommittedBitmap = new RoaringBitmap();

        this.versionsToBitmap = new AVLTree<>();
        this.currentVersion = initVersion;
        this.versionsToBitmap.insert(this.currentVersion, this.uncommittedBitmap);

//        this.myKeysToPos = new ArrayList<>(keysCapacity);
        this.keysToBitmapPositions = new HashMap<>(keysCapacity);
        this.bitmapPositionsToKeys = new HashMap<>(keysCapacity);
    }

    @Override
    public void commit(KVER nextVersion) throws Exception {
        Utils.checkVersion(this.currentVersion, nextVersion);
        this.currentVersion = nextVersion;
        this.uncommittedBitmap = this.uncommittedBitmap.clone();
        this.versionsToBitmap.insert(this.currentVersion, this.uncommittedBitmap);
    }

    @Override
    public void add(K key) throws Exception {
        Integer bitmapPos = this.keysToBitmapPositions.getOrDefault(key, null);
        if (bitmapPos == null) {
            bitmapPos = this.keysToBitmapPositions.size();
            this.keysToBitmapPositions.put(key, bitmapPos);
            this.bitmapPositionsToKeys.put(bitmapPos, key);
        }

        this.uncommittedBitmap.add(bitmapPos);
    }

    @Override
    public void deleteFromCurrentKeys(K key) throws Exception {
        Integer bitmapPos = this.keysToBitmapPositions.getOrDefault(key, null);
        if (bitmapPos != null) {
            this.uncommittedBitmap.remove(bitmapPos);
        } else {
            throw new Exception("key was not found");
        }
    }

    @Override
    public Set<K> getKeys(KVER version) throws Exception {
        Set<K> keys = new HashSet<>();
        RoaringBitmap bitMap = this.versionsToBitmap.search(version);
        populateKeys(bitMap, keys);
        return keys;
    }

    @Override
    public void populateKeys(RoaringBitmap bitMap, Set<K> keys) throws Exception {
        Utils.assertNotNull(keys, "should not be null");
        for (int pos : bitMap) {
            K key = this.bitmapPositionsToKeys.get(pos);
            keys.add(key);
        }
    }

    @Override
    public Set<K> getKeys(KVER verStart, KVER verEnd) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");

        RoaringBitmap bms = new RoaringBitmap();
        for (RoaringBitmap bm : this.versionsToBitmap.values(verStart, verEnd)) {
            bms.or(bm);
        }

        Set<K> keys = new HashSet<>();
        populateKeys(bms, keys);

        return keys;
    }


}
