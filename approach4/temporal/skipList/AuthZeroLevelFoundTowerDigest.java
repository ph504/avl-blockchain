package approach4.temporal.skipList;

import approach4.IAuthZeroLevelDigest;
import approach4.IRowDetails;
import approach4.Utils;
import approach4.temporal.temporalPartitions.PartitionSearchRes;

public class AuthZeroLevelFoundTowerDigest<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> implements IAuthZeroLevelDigest {
    public final K key;
    public final PartitionSearchRes<K,V,KVER> value;
    public final byte[] digest;

    final ToweredTypeUtils<K,V> toweredTypeUtils;

    AuthZeroLevelFoundTowerDigest(K key, PartitionSearchRes<K,V,KVER> value, ToweredTypeUtils<K, V> toweredTypeUtils) throws Exception {
        this.key = key;
        this.value = value;
        this.toweredTypeUtils = toweredTypeUtils;
        this.digest = getKeyValueDigest();
    }

    public byte[] getZeroLevelDigest(byte[] pointedTowerDigest) throws Exception {
        byte[] currentAndPointedTowerDigest = Utils.getHash(this.digest, pointedTowerDigest);
        return currentAndPointedTowerDigest;
    }

    public byte[] getDigest() throws Exception {
        return this.digest;
    }

    private byte[] getKeyValueDigest() throws Exception {
        byte[] curTowerKeyDigest = Utils.getNullableObjectHash(this.key, this.toweredTypeUtils.kTypeUtils);
        byte[] curTowerValueDigest = value.getDigest();
        byte[] digest = Utils.getHash(curTowerKeyDigest, curTowerValueDigest);
        return digest;
    }

    public V getElem(KVER version) throws Exception {
        return this.value.validateAndGetResults(version);
    }


}
