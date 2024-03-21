package approach4.temporal;

import approach4.IRowDetails;
import approach4.Utils;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.skipList.ToweredSkipList;
import approach4.temporal.skipList.ToweredTypeUtils;

import java.util.ArrayList;
import java.util.Set;

/**
 * @brief wrapping the skiplist with v2k mapper
 * @param <VersionType> is the version and is the Type of Date
 * @param <KeyType> is the key and is the Type of Integer
 * @param <BucketRowType> is the row from bucket list and is the Type of
 */
public class ExtendedMultiVersionSkipList
                <VersionType extends Comparable<VersionType>,
                KeyType extends Comparable<KeyType>,
                BucketRowType extends IRowDetails<KeyType,BucketRowType,VersionType>
                > {
    private VersionType currentVersion;
    final private ToweredSkipList<VersionType,KeyType,BucketRowType> toweredSkipList;
    final private IVersionsToKeysIndex<VersionType,KeyType> versionsToKeysIndex;
    final private int partitionCapacity;


    /**
     *
     * @param initVersion
     * @param iterationProbability
     * @param versionsToKeysIndex
     * @param partitionCapacity
     * @param toweredTypeUtils
     * @throws Exception
     */
    public ExtendedMultiVersionSkipList(
            VersionType initVersion,
            double iterationProbability,
            IVersionsToKeysIndex<VersionType,KeyType> versionsToKeysIndex,
            int partitionCapacity,
            ToweredTypeUtils<KeyType,BucketRowType> toweredTypeUtils)
                throws Exception {


        this.currentVersion = initVersion;
        this.partitionCapacity = partitionCapacity;
        this.toweredSkipList = new ToweredSkipList<>(
                initVersion,
                iterationProbability,
                this.partitionCapacity,
                toweredTypeUtils);
        this.versionsToKeysIndex = versionsToKeysIndex;
    }

    public void commitCurrentVersion(VersionType nextVersion) throws Exception {
        Utils.checkVersion(this.currentVersion, nextVersion);
        this.currentVersion = nextVersion;
        this.toweredSkipList.commitCurrentVersion(nextVersion);
        this.versionsToKeysIndex.commit(nextVersion);
    }

    public void insert(BucketRowType row) throws Exception {
        this.toweredSkipList.upsert(row);
        KeyType key = row.getKey();
        this.versionsToKeysIndex.add(key);
    }

    public void delete(KeyType key) throws Exception {
        this.toweredSkipList.delete(key);
        this.versionsToKeysIndex.deleteFromCurrentKeys(key);
    }

    public void update(BucketRowType row) throws Exception {
        this.toweredSkipList.upsert(row);
    }

    public BucketRowType search1(VersionType version, KeyType key) throws Exception {
        BucketRowType row = this.toweredSkipList.search1(version, key);
        return row;
    }

    public void rangeSearch1(VersionType version, KeyType keyStart, KeyType keyEnd, ArrayList<IRowDetails<KeyType,BucketRowType,VersionType>> rows) throws Exception {
        Utils.assertTrue(keyStart.compareTo(keyEnd) <= 0, "should be true");
        this.toweredSkipList.rangeSearch1(version,keyStart,keyEnd,rows);
    }

    public void rangeSearch2(VersionType verStart, VersionType verEnd, KeyType key, ArrayList<IRowDetails<KeyType,BucketRowType,VersionType>> rows) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        this.toweredSkipList.rangeSearch2(verStart,verEnd,key,rows);
    }

    public void rangeSearch3(VersionType verStart, VersionType verEnd, KeyType keyStart, KeyType keyEnd, ArrayList<IRowDetails<KeyType,BucketRowType,VersionType>> rows) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        this.toweredSkipList.rangeSearch3(verStart,verEnd,keyStart,keyEnd,rows);
    }

    public void rangeSearch4(VersionType verStart, VersionType verEnd, ArrayList<IRowDetails<KeyType,BucketRowType,VersionType>> rows) throws Exception {
        Utils.assertTrue(verStart.compareTo(verEnd) <= 0, "should be true");
        Set<KeyType> keys = this.versionsToKeysIndex.getKeys(verStart, verEnd);
        for (KeyType key : keys) {
            this.toweredSkipList.rangeSearch2(verStart, verEnd, key, rows);
        }
    }






}
