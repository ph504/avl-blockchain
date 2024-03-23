package approach4.valueDataStructures;

import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.Utils;

public class TableRowStrIntCols implements IRowDetails<String, TableRowStrIntCols, Integer> {
    private Version<Integer> version;
    private byte[] digest;
    public final String col1;
    public final int col2;
    private final ITypeUtils<TableRowStrIntCols> tableRowOneStrColClassUtils;


    public TableRowStrIntCols(String col1, int col2, ITypeUtils<TableRowStrIntCols> tableRowOneStrColClassUtils) {
        this.col1 = col1;
        this.col2 = col2;

        this.version = new Version<>(null, null);
        this.digest = null;

        this.tableRowOneStrColClassUtils = tableRowOneStrColClassUtils;
    }

    @Override
    public Version<Integer> getVersion() {
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
    public TableRowStrIntCols clone() {
        return this;
    }


}
