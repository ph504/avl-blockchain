package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;
import approach4.valueDataStructures.TableRowTenCols;
import approach4.valueDataStructures.Version;

import java.nio.ByteBuffer;

public class TableRowTenColsClassUtils implements ITypeUtils<TableRowTenCols> {
    @Override
    public byte[] getZeroLevelDigest(TableRowTenCols obj) throws Exception {
        Version<Integer> objVersion = obj.getVersion();
        byte[] validFromDigest = ByteBuffer.allocate(4).putInt(objVersion.getValidFrom()).array();
        byte[] validToDigest = null;
        Integer validTo = objVersion.getValidTo();
        if (validTo == null) {
            validToDigest = Utils.nullDigest;
        } else {
            validToDigest = ByteBuffer.allocate(4).putInt(validTo).array();
        }

        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col2Digest = ByteBuffer.allocate(4).putInt(obj.col2).array();
        byte[] col3Digest = ByteBuffer.allocate(4).putInt(obj.col3).array();
        byte[] col4Digest = ByteBuffer.allocate(4).putInt(obj.col4).array();
        byte[] col5Digest = ByteBuffer.allocate(4).putInt(obj.col5).array();
        byte[] col6Digest = ByteBuffer.allocate(4).putInt(obj.col6).array();
        byte[] col7Digest = ByteBuffer.allocate(4).putInt(obj.col7).array();
        byte[] col8Digest = ByteBuffer.allocate(4).putInt(obj.col8).array();
        byte[] col9Digest = ByteBuffer.allocate(4).putInt(obj.col9).array();
        byte[] col10Digest = ByteBuffer.allocate(4).putInt(obj.col10).array();

        byte[] digest1 = Utils.getHash(validFromDigest, validToDigest);
        byte[] digest2 = Utils.getHash(col1Digest, col2Digest);
        byte[] digest3 = Utils.getHash(col3Digest, col4Digest);
        byte[] digest4 = Utils.getHash(col5Digest, col6Digest);
        byte[] digest5 = Utils.getHash(col7Digest, col8Digest);
        byte[] digest6 = Utils.getHash(col9Digest, col10Digest);

        byte[] digest7 = Utils.getHash(digest1, digest2);
        byte[] digest8 = Utils.getHash(digest3, digest4);
        byte[] digest9 = Utils.getHash(digest5, digest6);

        byte[] digest10 = Utils.getHash(digest7, digest8);
        byte[] digest11 = Utils.getHash(digest9, digest10);

        return digest11;
    }

    @Override
    public TableRowTenCols clone(TableRowTenCols obj) throws Exception {
        return obj;
    }



}
