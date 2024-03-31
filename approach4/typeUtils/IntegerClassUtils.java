package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class IntegerClassUtils implements ITypeUtils<Integer> {
    @Override
    public byte[] getZeroLevelDigest(Integer obj) {
        byte[] ba = ByteBuffer.allocate(4).putInt(obj).array();
        byte[] digest = Utils.hash(ba);
        return digest;
    }

    @Override
    public Integer clone(Integer obj) {
        return obj;
    }

    // TODO: Remove
    public static ArrayList<Integer> genSortedNums(int init, int step, int count) {
        // equivalent to range list in python.
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i = 0; i< count; i++) {
            list.add(cur);
            cur += step;
        }
        return list;
    }

}
