package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;
import approach4.valueDataStructures.TableRowStrLongCols;
import approach4.valueDataStructures.Version;

import java.nio.ByteBuffer;

public class TableRowStrLongColsClassUtils implements ITypeUtils<TableRowStrLongCols> {
    @Override
    public byte[] getZeroLevelDigest(TableRowStrLongCols obj) throws Exception {
        Version<Long> objVersion = obj.getVersion();
        byte[] validFromDigest = ByteBuffer.allocate(8).putLong(objVersion.getValidFrom()).array();
        byte[] validToDigest = null;
        Long validTo = objVersion.getValidTo();
        if (validTo == null) {
            validToDigest = Utils.nullDigest;
        } else {
            validToDigest = ByteBuffer.allocate(8).putLong(validTo).array();
        }

//        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col1Digest = obj.col1.getBytes();
        byte[] col2Digest = ByteBuffer.allocate(8).putLong(obj.col2).array();

        byte[] digest1 = Utils.getHash(validFromDigest, validToDigest);
        byte[] digest2 = Utils.getHash(col1Digest, col2Digest);
        byte[] digest3 = Utils.getHash(digest1, digest2);

        return digest3;
    }

    @Override
    public TableRowStrLongCols clone(TableRowStrLongCols obj) {
        return obj;
    }



}
