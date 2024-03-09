package approach4.temporal;

import approach4.CompositeKey;
import approach4.ITypeUtils;
import approach4.MPTPointerBased.Serializer;
import approach4.MPTPointerBased.Trie;
import approach4.Utils;
import approach4.typeUtils.IntegerClassUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PatriciaMerkleTrieMVCompositeIntDate {

    final private Trie<CompositeKey<Integer,Date>,Integer> mpt;
    final private ArrayList<CompositeKey<Integer,Date>> keysVersions;


    public PatriciaMerkleTrieMVCompositeIntDate() {
        ITypeUtils<Integer> intClassUtils = new IntegerClassUtils();
        this.mpt = new Trie<>(Serializer.COMPOSITE_KEY, intClassUtils);
        this.keysVersions = new ArrayList<>();

    }

    public void upsert(CompositeKey<Integer,Date> compositeKey, Integer value) throws Exception {
        this.keysVersions.add(new CompositeKey<>(compositeKey.k1, compositeKey.k2));
        this.mpt.put(compositeKey, value);
    }



    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Integer> values) throws Exception {
        for (CompositeKey<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.k1;
            Date ver = keysVersions.k2;
            if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                if (ver.compareTo(version) == 0) {
                    getIndexValue(ver, key, values);
                }

            }
        }
    }

    private void getIndexValue(Date ver, Integer key, List<Integer> values) throws Exception {
        CompositeKey<Integer, Date> compositeKey = new CompositeKey<>(key, ver);
        Integer value = this.mpt.get(compositeKey);
        Utils.assertNotNull(value, "should not be null");
        values.add(value);
    }

    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Integer> values) throws Exception {
        for (CompositeKey<Integer, Date> keysVersions : this.keysVersions) {
            Integer k = keysVersions.k1;
            Date version = keysVersions.k2;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (k.compareTo(key) == 0) {
                    getIndexValue(version, key, values);
                }

            }
        }
    }

    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Integer> values) throws Exception {
        for (CompositeKey<Integer, Date> keysVersions : this.keysVersions) {
            Integer key = keysVersions.k1;
            Date version = keysVersions.k2;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                    getIndexValue(version, key, values);
                }
            }
        }
    }

//    public void rangeSearch4(Integer verStart, Integer verEnd, List<Integer> values) throws Exception {
//        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
//            Integer version = keysVersions.first;
//            String key = keysVersions.second;
//            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
//                getIndexValue(version, key, values);
//            }
//        }
//    }
}
