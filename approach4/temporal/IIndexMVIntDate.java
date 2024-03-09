package approach4.temporal;

import approach4.IRowDetails;
import approach4.valueDataStructures.TableRowIntDateCols;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public interface IIndexMVIntDate {
    void commitCurrentVersion(Date nextVersion) throws Exception;

    void insert(TableRowIntDateCols row) throws Exception;

    void finalizeInsert() throws Exception;

    void delete(Integer key) throws Exception;

    void update(TableRowIntDateCols row) throws Exception;

    void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception;

    void rangeSearch1(Date version, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception;

    void rangeSearch2(Date verStart, Date verEnd, Integer key, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception;

    void rangeSearch2(Date verStart, Date verEnd, Integer key, List<Object> rows) throws Exception;

    void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception;

    void rangeSearch3(Date verStart, Date verEnd, Integer keyStart, Integer keyEnd, List<Object> rows) throws Exception;

    void rangeSearch4(Date verStart, Date verEnd, ArrayList<IRowDetails<Integer, TableRowIntDateCols, Date>> rows) throws Exception;

    void rangeSearch4(Date verStart, Date verEnd, List<Object> rows) throws Exception;
}
