package approach4.temporal.VersionToKey;

import org.roaringbitmap.RoaringBitmap;

import java.util.Set;

public interface IVersionsToKeysIndex<KVER extends Comparable<KVER>, K extends Comparable<K>> {
    void commit(KVER nextVersion) throws Exception;

    void add(K key) throws Exception;

    void deleteFromCurrentKeys(K key) throws Exception;

    Set<K> getKeys(KVER version) throws Exception;

    void populateKeys(RoaringBitmap bitMap, Set<K> keys) throws Exception;

    Set<K> getKeys(KVER verStart, KVER verEnd) throws Exception;
}
