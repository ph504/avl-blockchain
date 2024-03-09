package approach4.temporal.skipList;


import approach4.IAuthDigest;
import approach4.IRowDetails;
import approach4.Utils;

public class AuthZeroLevelDigest <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> implements IAuthDigest {
    public final K key;
    public final byte[] valueDigest;
    private final ToweredTypeUtils<K,V> toweredTypeUtils;

    public AuthZeroLevelDigest(K key, byte[] valueDigest, ToweredTypeUtils<K, V> toweredTypeUtils) {
        this.key = key;
        this.valueDigest = valueDigest;
        this.toweredTypeUtils = toweredTypeUtils;
    }

    public byte[] getZeroLevelDigest(byte[] pointedTowerDigest) throws Exception {
        byte[] curTowerKeyValueDigest = getKeyValueDigest();
        byte[] currentAndPointedTowerDigest = Utils.getHash(curTowerKeyValueDigest, pointedTowerDigest);
        return currentAndPointedTowerDigest;
    }

    //TODO this can be done by the server or by the client
    private byte[] getKeyValueDigest() throws Exception {
        byte[] curTowerKeyDigest = Utils.getNullableObjectHash(this.key, this.toweredTypeUtils.kTypeUtils);
        byte[] digest = Utils.getHash(curTowerKeyDigest, this.valueDigest);
        return digest;
    }
}
