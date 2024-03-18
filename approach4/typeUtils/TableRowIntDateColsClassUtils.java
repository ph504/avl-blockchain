package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.Utils;
import approach4.valueDataStructures.TableRowIntDateCols;
import approach4.valueDataStructures.Version;

import java.nio.ByteBuffer;
import java.util.Date;

public class TableRowIntDateColsClassUtils implements ITypeUtils<TableRowIntDateCols> {
    @Override
    public byte[] getZeroLevelDigest(TableRowIntDateCols obj) throws Exception {
        Version<Date> objVersion = obj.getVersion();
        byte[] validFromDigest = ByteBuffer.allocate(24).putLong(objVersion.getValidFrom().getTime()).array();
        byte[] validToDigest = null;
        Date validTo = objVersion.getValidTo();
        if (validTo == null) {
            validToDigest = Utils.nullDigest;
        } else {
            validToDigest = ByteBuffer.allocate(24).putLong(validTo.getTime()).array();
        }

//        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col2Digest = ByteBuffer.allocate(24).putLong(obj.col2.getTime()).array();

        byte[] digest1 = Utils.getHash(validFromDigest, validToDigest);
        byte[] digest2 = Utils.getHash(col1Digest, col2Digest);
        byte[] digest3 = Utils.getHash(digest1, digest2);

        return digest3;
    }

    @Override
    public TableRowIntDateCols clone(TableRowIntDateCols obj) {
        return obj;
    }



}