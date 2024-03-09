package approach4.valueDataStructures;

import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.Utils;

import java.util.Date;

public class TableRowIntDateCols implements IRowDetails<Integer, TableRowIntDateCols, Date> {
    private Version<Date> version;
    private byte[] digest;

    public final int col1;
    public final Date col2;

    private final ITypeUtils<TableRowIntDateCols> tableRowOneIntColClassUtils;


    public TableRowIntDateCols(Integer col1, Date col2, ITypeUtils<TableRowIntDateCols> tableRowOneIntColClassUtils) {
        this.col1 = col1;
        this.col2 = col2;

        this.version = new Version<>(null, null);
        this.digest = null;

        this.tableRowOneIntColClassUtils = tableRowOneIntColClassUtils;
    }

    @Override
    public Version<Date> getVersion() {
        return this.version;
    }

    @Override
    public void initVersions() {
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
        return tableRowOneIntColClassUtils.getZeroLevelDigest(this);
    }

    @Override
    public Integer getKey() {
        return this.col1;
    }



    @Override
    public TableRowIntDateCols clone() {
        return this;
    }


}
