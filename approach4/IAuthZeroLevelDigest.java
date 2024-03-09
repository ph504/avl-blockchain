package approach4;

public interface IAuthZeroLevelDigest extends IAuthDigest {
    byte[] getZeroLevelDigest(byte[] nextDigest) throws Exception;
    byte[] getDigest() throws Exception;

}
