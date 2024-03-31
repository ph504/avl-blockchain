package approach4.typeUtils;

import approach4.ITypeUtils;
import approach4.TupleTwo;
import approach4.Utils;
import approach4.temporal.skipList.ToweredTypeUtils;
import approach4.valueDataStructures.TableRowIntDateCols;
import approach4.valueDataStructures.Version;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A definition of a single row in the bucket list which is the partition class
 * @see approach4.temporal.temporalPartitions.Partition
 * Partition
 * @see approach4.valueDataStructures.TableRowIntDateCols
 * as it seems to be the wrapper
 */
public class TableRowUtils implements ITypeUtils<TableRowIntDateCols> {


    /**
     * the annoying thing about this function is that this shouldn't be in the bucket list row,
     * this should be in the tower levels I think.
     * @param obj
     * @return
     * @throws Exception
     */
    @Override
    public byte[] getZeroLevelDigest(TableRowIntDateCols obj) throws Exception {
        Version<Date> objVersion = obj.getVersion();
        byte[] validFromDigest = ByteBuffer.allocate(24).putLong(objVersion.getValidFrom().getTime()).array();
        byte[] validToDigest = null;
        Date validTo = objVersion.getValidTo();
        if (validTo == null) {
            validToDigest = Utils.nullDigest;
        } else {
            validToDigest = ByteBuffer.allocate(24).putLong(validTo.getTime()).array();
        }

//        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col1Digest = ByteBuffer.allocate(4).putInt(obj.col1).array();
        byte[] col2Digest = ByteBuffer.allocate(24).putLong(obj.col2.getTime()).array();

        byte[] digest1 = Utils.getHash(validFromDigest, validToDigest);
        byte[] digest2 = Utils.getHash(col1Digest, col2Digest);
        byte[] digest3 = Utils.getHash(digest1, digest2);

        return digest3;
    }

    public static List<Date> getColumns(ArrayList<TupleTwo<Integer, Date>> data) {
        // Get list of versions only
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) versions_.add(row.second);
        List<Date> res = versions_.stream().sorted(Comparator.comparing(o -> o)).collect(Collectors.toList());
        return res;
    }

    public static List<Date> getColumns(List<TableRowIntDateCols> data) {
        // Get list of versions only
        HashSet<Date> versions_ = new HashSet<>();
        for (TableRowIntDateCols row : data) versions_.add(row.col2);
        List<Date> res = versions_.stream().sorted(Comparator.comparing(o -> o)).collect(Collectors.toList());
        return res;
    }

    private static ArrayList<TupleTwo<Integer, Date>> getData(
            List<Date> versions,
            List<Integer> keys,
            int keysPerVersionCount) {

        ArrayList<TupleTwo<Integer, Date>> data = new ArrayList<>();
        for (Date date : versions) {
            // so that we can have random keys with repetition allowed asssigned to each version
            Collections.shuffle(keys);
            for (int i = 0; i< keysPerVersionCount; i++) {
                data.add(new TupleTwo<>(keys.get(i), date));
            }
        }
        return data;
    }

    public static List<TableRowIntDateCols> getTableRowIntDateCols(
            List<Date> versions,
            List<Integer> keys,
            int keysPerVersionCount) {
        ArrayList<TupleTwo<Integer, Date>> data = getData(versions, keys, keysPerVersionCount);

        ToweredTypeUtils<Integer, TableRowIntDateCols> tableRowUtils = getUtils();

        List<TableRowIntDateCols> data_ = wrapUtilData(data, tableRowUtils);

        return data_;
    }

    public static List<TableRowIntDateCols> getTableRowIntDateCols(
            List<Date> versions,
            List<Integer> keys,
            int keysPerVersionCount,
            ToweredTypeUtils<Integer, TableRowIntDateCols> tableRowUtils) {
        ArrayList<TupleTwo<Integer, Date>> data = getData(versions, keys, keysPerVersionCount);

        List<TableRowIntDateCols> data_ = wrapUtilData(data, tableRowUtils);

        return data_;
    }

    private static List<TableRowIntDateCols> wrapUtilData(
            ArrayList<TupleTwo<Integer, Date>> data,
            ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils) {
        // Convert data to type data_
        // which just contains the utils type extra to data.
        List<TableRowIntDateCols> data_ = new ArrayList<>(data.size());
        for (TupleTwo<Integer, Date> row : data) {
            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            data_.add(tr);
        }
        return data_;
    }

    public static ToweredTypeUtils<Integer, TableRowIntDateCols> getUtils() {
        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        ITypeUtils<TableRowIntDateCols> TableRowUtils = new TableRowUtils();
        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = new ToweredTypeUtils<>(integerClassUtils, TableRowUtils);
        return tableIntDateColsIndexTypeUtils;
    }

    public static ArrayList<Date> genVersions(int datesCount) {
        LocalDate startLocalDate = LocalDate.of(2024, Month.MARCH, 10);
        LocalDate currentLocalDate = startLocalDate;
        ZoneId defaultZoneId = ZoneId.systemDefault();

        // Dates starting from 2015 until datesCount goes to zero
        ArrayList<Date> sequentialDates = new ArrayList<>();

        while (datesCount > 0) {
            Date currentDate = Date.from(currentLocalDate.atStartOfDay(defaultZoneId).toInstant());
            sequentialDates.add(currentDate);
            currentLocalDate = currentLocalDate.plusDays(1);
            datesCount--;
        }
        return sequentialDates;
    }

    @Override
    public TableRowIntDateCols clone(TableRowIntDateCols obj) {
        return obj;
    }
}
