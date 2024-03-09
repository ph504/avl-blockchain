package approach4.temporal.skipList;

import approach4.ITypeUtils;

public class ToweredTypeUtils<K extends Comparable<K>,V> {
    public final ITypeUtils<K> kTypeUtils;
    public final ITypeUtils<V> vTypeUtils;

    public ToweredTypeUtils(ITypeUtils<K> kTypeUtils, ITypeUtils<V> vTypeUtils) {
        this.kTypeUtils = kTypeUtils;
        this.vTypeUtils = vTypeUtils;
    }
}
