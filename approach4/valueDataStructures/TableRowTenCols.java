package approach4.valueDataStructures;

import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.Utils;

public class TableRowTenCols implements IRowDetails<Integer, TableRowTenCols, Integer> {
    private Version<Integer> version;
    private byte[] digest;

    public final int col1;
    public final int col2;
    public final int col3;
    public final int col4;
    public final int col5;
    public final int col6;
    public final int col7;
    public final int col8;
    public final int col9;
    public final int col10;

    private final ITypeUtils<TableRowTenCols> tableRowTenColsClassUtils;

    public TableRowTenCols(int col1, int col2, int col3, int col4, int col5, int col6, int col7, int col8, int col9, int col10, ITypeUtils<TableRowTenCols> tableRowTenColsClassUtils) {
        this.col1 = col1;
        this.col2 = col2;
        this.col3 = col3;
        this.col4 = col4;
        this.col5 = col5;
        this.col6 = col6;
        this.col7 = col7;
        this.col8= col8;
        this.col9= col9;
        this.col10= col10;

        this.version = new Version<>(null, null);
        this.digest = null;

        this.tableRowTenColsClassUtils = tableRowTenColsClassUtils;
    }

    @Override
    public Version<Integer> getVersion() throws Exception {
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
        return tableRowTenColsClassUtils.getZeroLevelDigest(this);
    }

    @Override
    public Integer getKey() {
        return this.col1;
    }



    @Override
    public TableRowTenCols clone() {
        return this;
    }
}
