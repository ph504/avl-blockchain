package approach4.valueDataStructures;

import approach4.IRowDetails;
import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.Utils;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.typeUtils.IntegerClassUtils;
import approach4.typeUtils.TableRowUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This seems an encapsulation of the {Bucket DS row} together with {version and digests}.
 * however the tableRowOneIntColClassUtils doesn't make any sense because it doesn't have any properties of the table row
 * seems that it just wanted to use the col1 and col2 of that DS.
 */
public class TableRowIntDateCols implements IRowDetails<Integer, TableRowIntDateCols, Date> {
    // version valid from and to
    private Version<Date> version;
    private byte[] digest;

    // the key
    public final int col1;
    // the current version
    public final Date col2;

    // if you found out what this class/object does let me know. IHNFI
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

    @Override
    public String toString() {
//        return  version.toString() +
//                ", digest=" + Arrays.toString(digest) +
                 return "\n\tkey=" + col1+
                ",\n\tcol2=" + col2 ;
//                ", tableRowOneIntColClassUtils=" + tableRowOneIntColClassUtils;
    }
}
