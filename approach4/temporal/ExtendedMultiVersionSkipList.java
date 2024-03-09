package approach4.temporal;

import approach4.IRowDetails;
import approach4.Utils;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.skipList.ToweredSkipList;
import approach4.temporal.skipList.ToweredTypeUtils;

import java.util.ArrayList;
import java.util.Set;

public class ExtendedMultiVersionSkipList<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> {
    private KVER currentVersion;
    final private ToweredSkipList<KVER,K,V> toweredSkipList;
    final private IVersionsToKeysIndex<KVER,K> versionsToKeysIndex;
    final private int partitionCapacity;



    public ExtendedMultiVersionSkipList(KVER initVersion, double iterationProbability, IVersionsToKeysIndex<KVER,K> versionsToKeysIndex, int partitionCapacity, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
        this.currentVersion = initVersion;
        this.partitionCapacity = partitionCapacity;
        this.toweredSkipList = new ToweredSkipList<>(initVersion, iterationProbability, this.partitionCapacity, toweredTypeUtils);
        this.versionsToKeysIndex = versionsToKeysIndex;
    }

    public void commitCurrentVersion(KVER nextVersion) throws Exception {
        Utils.checkVersion(this.currentVersion, nextVersion);
        this.currentVersion = nextVersion;
        this.toweredSkipList.commitCurrentVersion(nextVersion);
        this.versionsToKeysIndex.commit(nextVersion);
    }

    public void insert(V row) throws Exception {
        this.toweredSkipList.upsert(row);
        K key = row.getKey();
        this.versionsToKeysIndex.add(key);
    }

    public void delete(K key) throws Exception {
        this.toweredSkipList.delete(key);
        this.versionsToKeysIndex.deleteFromCurrentKeys(key);
    }

    public void update(V row) throws Exception {
        this.toweredSkipList.upsert(row);
    }

    public V search1(KVER version, K key) throws Exception {
        V row = this.toweredSkipList.search1(version, key);
        return row;
    }

    public void rangeSearch1(KVER version, K keyStart, K keyEnd, ArrayList<IRowDetails<K,V,KVER>> rows) throws Exception {
        Utils.assertTrue(keyStart.compareTo(keyEnd) <= 0, "should be true");
        this.toweredSkipList.rangeSearch1(version,keyStart,keyEnd,rows);
    }

    public void rangeSearch2(KVER verStart, KVER verEnd, K key, ArrayList<IRowDetails<K,V,KVER>> rows) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        this.toweredSkipList.rangeSearch2(verStart,verEnd,key,rows);
    }

    public void rangeSearch3(KVER verStart, KVER verEnd, K keyStart, K keyEnd, ArrayList<IRowDetails<K,V,KVER>> rows) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        this.toweredSkipList.rangeSearch3(verStart,verEnd,keyStart,keyEnd,rows);
    }

    public void rangeSearch4(KVER verStart, KVER verEnd, ArrayList<IRowDetails<K,V,KVER>> rows) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        Set<K> keys = this.versionsToKeysIndex.getKeys(verStart, verEnd);
        for (K key : keys) {
            this.toweredSkipList.rangeSearch2(verStart, verEnd, key, rows);
        }
    }






}
