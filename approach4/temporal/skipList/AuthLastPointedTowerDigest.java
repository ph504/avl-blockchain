package approach4.temporal.skipList;


import approach4.IAuthDigest;

public class AuthLastPointedTowerDigest implements IAuthDigest {
    public final byte[] digest;

    public AuthLastPointedTowerDigest(byte[] digest) {
        this.digest = digest;
    }

    public byte[] getZeroLevelDigest() {
        return this.digest;
    }
}
