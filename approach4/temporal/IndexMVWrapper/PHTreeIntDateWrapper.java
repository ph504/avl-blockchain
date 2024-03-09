package approach4.temporal.IndexMVWrapper;

import approach4.IRowDetails;
import approach4.temporal.IIndexMVIntDate;
import approach4.temporal.MerkleBucketTreeMVIntDate;
import approach4.valueDataStructures.TableRowIntDateCols;
import org.tinspin.index.PointEntry;
import org.tinspin.index.QueryIterator;
import org.tinspin.index.phtree.PHTreeP;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PHTreeIntDateWrapper implements IIndexMVIntDate {

    private PHTreeP<Integer> index = null;


    public PHTreeIntDateWrapper() {
        this.index = PHTreeP.createPHTree(2);
    }

    @Override
    public void commitCurrentVersion(Date nextVersion) {
        // do nothing
    }

    @Override
    public void insert(TableRowIntDateCols row) throws Exception {
        double versionDouble = row.col2.getTime();
        this.index.insert(new double[]{versionDouble, row.col1}, row.col1);
    }

    @Override
    public void finalizeInsert() throws Exception {
        // do nothing
    }

    @Override
    public void delete(Integer key) throws Exception {
        throw new Exception("delete not supported in PH Tree index");
    }

    @Override
    public void update(TableRowIntDateCols row) throws Exception {
        throw new Exception("update not supported in PH Tree index");
    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing

    }

    @Override
    public void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {
        double versionDouble = version.getTime();

        QueryIterator<PointEntry<Integer>> it = index.query(new double[]{versionDouble,keyStart}, new double[]{versionDouble,keyEnd});

        while (it.hasNext()) {
            PointEntry<Integer> next = it.next();
            rows.add(next.value());
        }
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> rows) throws Exception {
        double verStartDouble = verStart.getTime();
        double verEndDouble = verEnd.getTime();

        QueryIterator<PointEntry<Integer>> it = index.query(new double[]{verStartDouble,key}, new double[]{verEndDouble,key});

        while (it.hasNext()) {
            PointEntry<Integer> next = it.next();
            rows.add(next.value());
        }
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {
        double verStartDouble = verStart.getTime();
        double verEndDouble = verEnd.getTime();

        QueryIterator<PointEntry<Integer>> it = index.query(new double[]{verStartDouble,keyStart}, new double[]{verEndDouble,keyEnd});

        while (it.hasNext()) {
            PointEntry<Integer> next = it.next();
            rows.add(next.value());
        }
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception {
        double verStartDouble = verStart.getTime();
        double verEndDouble = verEnd.getTime();

        QueryIterator<PointEntry<Integer>> it = index.query(new double[]{verStartDouble,Double.MIN_VALUE}, new double[]{verEndDouble,Double.MAX_VALUE});

        while (it.hasNext()) {
            PointEntry<Integer> next = it.next();
            rows.add(next.value());
        }
    }


}
