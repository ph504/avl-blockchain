package approach4.temporal;

import approach4.ITypeUtils;
import approach4.MPTPointerBased.Serializer;
import approach4.MPTPointerBased.Trie;
import approach4.TupleTwo;
import approach4.typeUtils.IntegerClassUtils;
import approach4.Utils;


import java.util.ArrayList;
import java.util.List;

public class PatriciaMerkleTrieMVStringInteger {

    final private Trie<String,Integer> mpt;
    final private ArrayList<TupleTwo<Integer, String>> keysVersions;


    public PatriciaMerkleTrieMVStringInteger() {
        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        this.mpt = new Trie<>(Serializer.STRING_UTF8, integerClassUtils);
        this.keysVersions = new ArrayList<>();

    }

    public void upsert(Integer version, String key, Integer value) throws Exception {
        this.keysVersions.add(new TupleTwo<>(version, key));
        String compositeKey = key + "|" + version;
        this.mpt.put(compositeKey, value);
    }



    public void rangeSearch1(Integer version, String keyStart, String keyEnd, List<Integer> values) throws Exception {
        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
            Integer ver = keysVersions.first;
            String key = keysVersions.second;
            if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                if (ver.compareTo(version) == 0) {
                    getIndexValue(ver, key, values);
                }

            }
        }
    }

    private void getIndexValue(Integer ver, String key, List<Integer> values) throws Exception {
        String compositeKey = key + "|" + ver;
        Integer value = this.mpt.get(compositeKey);
        Utils.assertNotNull(value, "should not be null");
        values.add(value);
    }

    public void rangeSearch2(Integer verStart, Integer verEnd, String key, List<Integer> values) throws Exception {
        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
            Integer version = keysVersions.first;
            String k = keysVersions.second;
            if (version.compareTo(verStart) >= 0 && version.compareTo(verEnd) <= 0) {
                if (k.compareTo(key) == 0) {
                    getIndexValue(version, key, values);
                }

            }
        }
    }

    public void rangeSearch3(Integer verStart, Integer verEnd, String keyStart, String keyEnd, List<Integer> values) throws Exception {
        for (TupleTwo<Integer, String> keysVersions : this.keysVersions) {
            Integer version = keysVersions.first;
            String key = keysVersions.second;
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
