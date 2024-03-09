package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;
import approach4.valueDataStructures.TableRowStrIntCols;
import approach4.valueDataStructures.Version;

import java.nio.ByteBuffer;

public class TableRowStrIntColsClassUtils implements ITypeUtils<TableRowStrIntCols> {
    @Override
    public byte[] getZeroLevelDigest(TableRowStrIntCols obj) throws Exception {
        Version<Integer> objVersion = obj.getVersion();
        byte[] validFromDigest = ByteBuffer.allocate(4).putInt(objVersion.getValidFrom()).array();
        byte[] validToDigest = null;
        Integer validTo = objVersion.getValidTo();
        if (validTo == null) {
            validToDigest = Utils.nullDigest;
        } else {
            validToDigest = ByteBuffer.allocate(4).putInt(validTo).array();
        }

//        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col1Digest = obj.col1.getBytes();
        byte[] col2Digest = ByteBuffer.allocate(4).putInt(obj.col2).array();

        byte[] digest1 = Utils.getHash(validFromDigest, validToDigest);
        byte[] digest2 = Utils.getHash(col1Digest, col2Digest);
        byte[] digest3 = Utils.getHash(digest1, digest2);

        return digest3;
    }

    @Override
    public TableRowStrIntCols clone(TableRowStrIntCols obj) {
        return obj;
    }



}
