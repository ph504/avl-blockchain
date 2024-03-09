package approach4.temporal.skipList;


import approach4.IAuthDigest;
import approach4.Utils;

public class AuthUpperLevelDigest implements IAuthDigest {
    public final byte[] digest;

    public AuthUpperLevelDigest(byte[] digest) {
        this.digest = digest;
    }

    public byte[] getUpperLevelDigest(byte[] bottomTowerDigest) {
        byte[] rollingDigest = Utils.getHash(this.digest, bottomTowerDigest);
        return rollingDigest;
    }


}
