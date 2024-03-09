package approach4.valueDataStructures;

import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.Utils;

public class TableRowStrLongCols implements IRowDetails<String, TableRowStrLongCols, Long> {
    private Version<Long> version;
    private byte[] digest;

    public final String col1;
    public final long col2;

    private final ITypeUtils<TableRowStrLongCols> tableRowOneStrColClassUtils;


    public TableRowStrLongCols(String col1, long col2, ITypeUtils<TableRowStrLongCols> tableRowOneStrColClassUtils) {
        this.col1 = col1;
        this.col2 = col2;

        this.version = new Version<>(null, null);
        this.digest = null;

        this.tableRowOneStrColClassUtils = tableRowOneStrColClassUtils;
    }

    @Override
    public Version<Long> getVersion() {
        return this.version;
    }

    @Override
    public void initVersions() throws Exception {
        this.version = new Version<>(null,null);
    }

    @Override
    public byte[] getDigest() throws Exception {
        if (this.digest == null) {
            throw new Exception("digest was not calculated before query");
        }
        return this.digest;
    }

    @Override
    public byte[] calculateDigest(byte[] prevDigest) throws Exception {
        byte[] zeroLevelHash = getZeroLevelDigest();
        this.digest = Utils.getHash(zeroLevelHash, prevDigest);
        return this.digest;
    }

    @Override
    public byte[] getZeroLevelDigest() throws Exception {
        return tableRowOneStrColClassUtils.getZeroLevelDigest(this);
    }

    @Override
    public String getKey() {
        return this.col1;
    }



    @Override
    public TableRowStrLongCols clone() {
        return this;
    }


}
