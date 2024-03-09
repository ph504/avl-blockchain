package approach4.temporal.VersionToKey;

import approach4.AVL.AVLTree;
import approach4.Utils;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class VersionsToConsecutiveKeysIndex<KVER extends Comparable<KVER>, K extends Comparable<K>> implements IVersionsToKeysIndex<KVER, K> {
    private KVER currentVersion;
//    final private GenericBST<KVER, RoaringBitmap> versionsToBitmap;
    final private AVLTree<KVER,RoaringBitmap> versionsToBitmap;
    private RoaringBitmap uncommittedBitmap;
    final private ArrayList<K> keysToPos;

    public VersionsToConsecutiveKeysIndex(KVER initVersion, int keysCapacity) {
        this.uncommittedBitmap = new RoaringBitmap();

        this.versionsToBitmap = new AVLTree<>();
        this.currentVersion = initVersion;
        this.versionsToBitmap.insert(this.currentVersion, this.uncommittedBitmap);

        this.keysToPos = new ArrayList<>(keysCapacity);
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
        Integer pos = Utils.binarySearch(this.keysToPos, key);
        if (pos != null) {
            this.uncommittedBitmap.add(pos);
        } else {
            if (this.keysToPos.size() > 0) {
                Utils.checkStrictlyAscending(this.keysToPos.get(this.keysToPos.size() - 1), key);
            }

            this.keysToPos.add(key);
            this.uncommittedBitmap.add(this.keysToPos.size() - 1);
        }
    }

    @Override
    public void deleteFromCurrentKeys(K key) throws Exception {
        Integer pos = Utils.binarySearch(this.keysToPos, key);
        if (pos != null) {
            this.uncommittedBitmap.remove(pos);
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
        Iterator<Integer> it = bitMap.iterator();
        while (it.hasNext()) {
            int pos = it.next();
            K key = this.keysToPos.get(pos);
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
