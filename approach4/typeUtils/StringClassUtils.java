package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;

public class StringClassUtils implements ITypeUtils<String> {
    @Override
    public byte[] getZeroLevelDigest(String obj) {
        byte[] ba = obj.getBytes();
        byte[] digest = Utils.hash(ba);
        return digest;
    }

    @Override
    public String clone(String obj) {
        return obj;
    }



}
