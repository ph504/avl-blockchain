package approach4;

import approach4.valueDataStructures.Version;

/**
 *
 * @param <K> = K
 * @param <T> = V which means row I guess................
 * @param <V> = KVER
 *              THESE NAMINGS ARE INSANE HOW CAN YOU COMPLICATE SOMETHING THAT IS NOT??? OMFG!!!!!!
 */
//V extends IRowDetails<K,V,KVER>
public interface IRowDetails<K extends Comparable<K>, T, V extends Comparable<V>> {

    // TODO split to getValidFrom and getValidTo
    Version<V> getVersion() throws Exception;
    void initVersions() throws Exception;
    byte[] getDigest() throws Exception;
    byte[] calculateDigest(byte[] prevDigest) throws Exception;
    byte[] getZeroLevelDigest() throws Exception;
    K getKey();
    T clone() throws Exception;
}
