package approach4.temporal.IndexMVWrapper;

import approach4.IRowDetails;
import approach4.temporal.ExtendedMultiVersionSkipList;
import approach4.temporal.IIndexMVIntDate;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SkipListMVIntDateWrapper implements IIndexMVIntDate {

    private ExtendedMultiVersionSkipList<Date, Integer, TableRowIntDateCols> index = null;

    public SkipListMVIntDateWrapper(Date initVersion, double iterationProbability, IVersionsToKeysIndex<Date,Integer> versionsToKeysIndex, int partitionCapacity, ToweredTypeUtils<Integer, TableRowIntDateCols> toweredTypeUtils) throws Exception {
        this.index = new ExtendedMultiVersionSkipList<>(initVersion, iterationProbability, versionsToKeysIndex, partitionCapacity, toweredTypeUtils);
    }

    @Override
    public void commitCurrentVersion(Date nextVersion) throws Exception {
        this.index.commitCurrentVersion(nextVersion);
    }

    @Override
    public void insert(TableRowIntDateCols row) throws Exception {
        this.index.insert(row);
    }

    @Override
    public void finalizeInsert() throws Exception {
        // do nothing
    }

    @Override
    public void delete(Integer key) throws Exception {
        this.index.delete(key);
    }

    @Override
    public void update(TableRowIntDateCols row) throws Exception {
        this.index.update(row);
    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        this.index.rangeSearch1(version, keyStart,keyEnd, rows);
    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {

    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        this.index.rangeSearch2(verStart, verEnd, key, rows);
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> rows) throws Exception {
        // do nothing
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        this.index.rangeSearch3(verStart, verEnd, keyStart, keyEnd, rows);
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {
        // do nothing
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        this.index.rangeSearch4(verStart, verEnd, rows);
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception {
        // do nothing
    }


}
