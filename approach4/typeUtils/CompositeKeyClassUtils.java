package approach4.typeUtils;

import approach4.CompositeKey;
import approach4.ITypeUtils;
import approach4.Utils;

import java.nio.ByteBuffer;
import java.util.Date;

public class CompositeKeyClassUtils implements ITypeUtils<CompositeKey<Integer,Date>> {
    @Override
    public byte[] getZeroLevelDigest(CompositeKey<Integer,Date> obj) {
        ByteBuffer bb = ByteBuffer.allocate(24+4);
        bb.putLong(0, obj.k2.getTime());
        bb.putInt(24, obj.k1);
        byte[] ba = bb.array();
        byte[] digest = Utils.hash(ba);
        return digest;
    }

    @Override
    public CompositeKey<Integer,Date> clone(CompositeKey<Integer,Date> obj) {
        return obj;
    }



}
