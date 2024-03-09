package approach4.temporal.IndexMVWrapper;

import approach4.IRowDetails;
import approach4.MerkleKDTree.MerkleKDTree;
import approach4.temporal.IIndexMVIntDate;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MerkleKDTreeIntDateWrapper implements IIndexMVIntDate {

    private MerkleKDTree index = null;
    private ArrayList<double[]> data;


    public MerkleKDTreeIntDateWrapper() {
        data = new ArrayList<>();
    }

    @Override
    public void commitCurrentVersion(Date nextVersion) {
        // do nothing
    }

    @Override
    public void insert(TableRowIntDateCols row) throws Exception {
        double versionDouble = row.col2.getTime();
        this.data.add(new double[]{versionDouble, row.col1});
    }

    @Override
    public void finalizeInsert() throws Exception {
        double[][] da = new double[this.data.size()][];

        for (int i = 0; i < this.data.size(); i++) {
            da[i] = this.data.get(i);
        }

        this.index = new MerkleKDTree(da, 2);
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
        index.range(new double[]{versionDouble, keyStart}, new double[]{versionDouble, keyEnd}, rows);
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> rows) throws Exception {
        double verStartDouble = verStart.getTime();
        double verEndDouble = verEnd.getTime();

        index.range(new double[]{verStartDouble, key}, new double[]{verEndDouble, key}, rows);
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception {
        double verStartDouble = verStart.getTime();
        double verEndDouble = verEnd.getTime();

        index.range(new double[]{verStartDouble, keyStart}, new double[]{verEndDouble, keyEnd}, rows);
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception {
        //do nothing
    }

    @Override
    public void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception {
        double verStartDouble = verStart.getTime();
        double verEndDouble = verEnd.getTime();

        index.range(new double[]{verStartDouble, Double.MIN_VALUE}, new double[]{verEndDouble, Double.MAX_VALUE}, rows);
    }


}
