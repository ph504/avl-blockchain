package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;

import java.nio.ByteBuffer;

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



}
