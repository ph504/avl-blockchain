package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;

import java.nio.ByteBuffer;
import java.util.Date;

public class DateClassUtils implements ITypeUtils<Date> {
    @Override
    public byte[] getZeroLevelDigest(Date obj) {
        byte[] ba = ByteBuffer.allocate(24).putLong(obj.getTime()).array();
        byte[] digest = Utils.hash(ba);
        return digest;
    }

    @Override
    public Date clone(Date obj) {
        return obj;
    }



}
