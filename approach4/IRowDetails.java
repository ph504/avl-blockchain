package approach4;

import approach4.valueDataStructures.Version;

public interface IRowDetails<K extends Comparable<K>, T, V extends Comparable<V>> {

    //TODO split to getValidFrom and getValidTo
    Version<V> getVersion() throws Exception;
    void initVersions() throws Exception;

    byte[] getDigest() throws Exception;
    byte[] calculateDigest(byte[] prevDigest) throws Exception;
    byte[] getZeroLevelDigest() throws Exception;
    K getKey();
    T clone() throws Exception;
}
