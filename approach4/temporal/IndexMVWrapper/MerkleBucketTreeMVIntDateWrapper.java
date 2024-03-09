package approach4.temporal.IndexMVWrapper;

import approach4.IRowDetails;
import approach4.temporal.IIndexMVIntDate;
import approach4.temporal.MerkleBucketTreeMVIntDate;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MerkleBucketTreeMVIntDateWrapper implements IIndexMVIntDate {

    private MerkleBucketTreeMVIntDate index = null;

    public MerkleBucketTreeMVIntDateWrapper(int capacity) throws Exception {
        this.index = new MerkleBucketTreeMVIntDate(capacity);
    }

    @Override
    public void commitCurrentVersion(Date nextVersion) throws Exception {
        // do nothing
    }

    @Override
    public void insert(TableRowIntDateCols row) throws Exception {
        this.index.upsert(row.col2, row.col1, row.col1);
    }

    @Override
    public void finalizeInsert() throws Exception {
        // do nothing
    }

    @Override
    public void delete(Integer key) throws Exception {
        throw new Exception("delete not supported in Merkle Bucket Tree index");
    }

    @Override
    public void update(TableRowIntDateCols row) throws Exception {
        this.index.upsert(row.col2, row.col1, row.col1);
    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {
        this.index.rangeSearch1(version, keyStart,keyEnd, rows);
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> rows) throws Exception {
        this.index.rangeSearch2(verStart, verEnd, key, rows);
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {
        this.index.rangeSearch3(verStart, verEnd, keyStart, keyEnd, rows);
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception {
        this.index.rangeSearch4(verStart, verEnd, rows);
    }


}
