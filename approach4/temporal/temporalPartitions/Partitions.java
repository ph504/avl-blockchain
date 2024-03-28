package approach4.temporal.temporalPartitions;

import approach4.Utils;
import approach4.IRowDetails;
import approach4.valueDataStructures.Version;

import java.util.ArrayList;

public class Partitions<K extends Comparable<K>, T extends IRowDetails<K,T,V> , V extends Comparable<V>> {
    private final ArrayList<Partition<K,T,V>> partitions;
    private Partition<K,T,V> lastPartition = null;
    private final int partitionCapacity;
    public int elemCount = 0;


    public boolean isEmpty() {
        return this.elemCount == 0;
    }

    public Partitions(int partitionCapacity) throws Exception {
//        Utils.assertTrue(partitionCapacity >= 1, "should be true");

        this.partitionCapacity = partitionCapacity;
        this.partitions = new ArrayList<>();
    }

    public int getPosOfRowWithVersionEqualOrSmallerThan(V version) throws Exception {
        if (this.partitions.size() == 0) {
            return -1;
        }

        int start = 0;
        int end = this.partitions.size() - 1;

        int mid = -1;
        while (start <= end) {
            mid = start + ((end - start) / 2);

            Partition<K,T,V> partition = this.partitions.get(mid);
            if (partition.compareTo(version) == 0) {
                break;
            } else if (partition.compareTo(version) < 0) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }

        int posRes = mid;
        if (start <= end) {
            Partition<K,T,V> partition = this.partitions.get(posRes);
            int rowPos = partition.getPosEqualOrSmallerPos(version);
            posRes = (posRes * this.partitionCapacity) + rowPos;
        } else {
            if (start == this.partitions.size()) {
                posRes = this.elemCount - 1;
            } else if (end < 0) {
                posRes = 0;
            } else {
                Partition<K,T,V> partition = this.partitions.get(end);
                posRes = (end  * this.partitionCapacity) + partition.size() - 1;
            }
        }

        return posRes;
    }

    public byte[] getRootDigest() throws Exception {
        if (this.lastPartition == null) {
            return Utils.nullDigest;
        }

        T lastRowInPartition = this.lastPartition.getLastRow();
        return lastRowInPartition.getDigest();
    }

    public void addNewPartition() throws Exception {
        this.lastPartition = new Partition<>(this.partitionCapacity);
        this.partitions.add(this.lastPartition);
    }

//    void verifyCanAddNewRow(Version<V> lastRowVersion, Version<V> newRowVersion) throws Exception {
//
//        if (lastRowVersion.getValidFrom().compareTo(newRowVersion.getValidFrom()) >= 0) {
//            throw new Exception("inserted rows' versions should be unique and ascending");
//        }
//        if (newRowVersion.getValidTo() != null) {
//            throw new Exception("inserted rows should have null delete version");
//        }
//        if (lastRowVersion.getValidTo() == null) {
//            throw new Exception("row with the same key already exists in the current version");
//        }
//        if (lastRowVersion.getValidTo().compareTo(newRowVersion.getValidFrom()) > 0) {
//            throw new Exception("last row delete version can't be larger than new row create version");
//        }
//    }

    public T add(V currentVersion, T newRow) throws Exception {

        Version<V> newRowVersion = newRow.getVersion();
        newRowVersion.setValidFrom(currentVersion);
        if (this.partitions.size() > 0) {
            if (!this.lastPartition.isFull()) {
//                Utils.assertTrue(!this.lastPartition.isEmpty(), "should be true");
//                verifyCanAddNewRow(this.lastPartition.getLastRowVersion(), newRowVersion);

                T lastRowInPartition = this.lastPartition.getLastRow();
                byte[] lastRowDigest = lastRowInPartition.getDigest();
                newRow.calculateDigest(lastRowDigest);
            } else {
                T lastRowInPrevPartition = this.lastPartition.getLastRow();
//                verifyCanAddNewRow(lastRowInPrevPartition.getVersion(), newRowVersion);

                addNewPartition();

                byte[] lastRowDigestInPrevPartition = lastRowInPrevPartition.getDigest();
                newRow.calculateDigest(lastRowDigestInPrevPartition);
            }
        } else {
            addNewPartition();
            newRow.calculateDigest(Utils.nullDigest);
        }

        this.lastPartition.add(newRow);
        this.elemCount++;

        return newRow;
    }

//    public static <K extends Comparable<K>, T extends IRowDetails<K,T,V>, V extends Comparable<V>>
//    int getPosOfLargestPartitionSmallerThan(Partitions<K,T,V> partitions, V elemVersion) throws Exception {
//
//        if (partitions.partitions.size() == 0) {
//            return -1;
//        }
//
//        int start = 0;
//        int end = partitions.size() - 1;
//
//        int mid = -1;
//        while(start <= end) {
//            mid = start + ((end - start) / 2);
//
//            Partition<K,T,V> partition = partitions.partitions.get(mid);
//            if (partition.compareTo(elemVersion) == 0) {
//                mid = mid - 1;
//                break;
//            } else if (partition.compareTo(elemVersion) < 0) {
//                start = mid + 1;
//            } else {
//                end = mid - 1;
//            }
//        }
//
//        if (start > end) {
//            if (start == partitions.size()) {
//                mid = partitions.size() - 1;
//            } else if (end < 0) {
//                mid = -1;
//            } else {
//                mid = end;
//            }
//        }
//
//        return mid;
//    }
//
//    int getPosOfLargestPartitionSmallerThan(V elemVersion) throws Exception {
//        return getPosOfLargestPartitionSmallerThan(this, elemVersion);
//    }

//    public int getRowWithVersionEqualOrBiggerPos(V version) throws Exception {
//        int posRes = this.getRowWithVersionEqualOrBiggerPos1(version);
//        return posRes;
//    }

    public void
    search(V verStart, V verEnd, ArrayList<IRowDetails<K,T,V>> foundRows) throws Exception {
//        Utils.assertTrue(verStart.compareTo(verEnd) <= 0);

        if (this.isEmpty()) {
            return;
        }

        int posRes = this.getPosOfRowWithVersionEqualOrSmallerThan(verStart);
        int foundPartitionPos = posRes / this.partitionCapacity;
        int foundRowPos = posRes % this.partitionCapacity;

        while (foundPartitionPos < this.partitions.size()) {
            Partition<K, T, V> partition = this.partitions.get(foundPartitionPos);
            while (foundRowPos < partition.size()) {
                T runningRow = partition.get(foundRowPos);
                Version<V> rowVersion = runningRow.getVersion();

                if (rowVersion.getValidFrom().compareTo(verEnd) > 0) {
                    return;
                }

                if (rowVersion.getValidTo() == null) {
                    foundRows.add(runningRow);
                } else {
                    if (rowVersion.getValidTo().compareTo(verStart) > 0) {
                        foundRows.add(runningRow);
                    }
                }

                foundRowPos++;
            }
            foundRowPos = 0;
            foundPartitionPos++;
        }
    }

    public T search(V version) throws Exception {
        if (this.isEmpty()) {
            return null;
        }

        T lastRow = this.getLastRow();
        Version<V> lastRowVer = lastRow.getVersion();
        if (lastRowVer.getValidFrom().compareTo(version) <= 0) {
            if ((lastRowVer.getValidTo() == null) || (lastRowVer.getValidTo().compareTo(version) > 0)) {
                return lastRow;
            }
        }

        int posRes = this.getPosOfRowWithVersionEqualOrSmallerThan(version);
        int foundPartitionPos = posRes / this.partitionCapacity;
        int foundRowPos = posRes % this.partitionCapacity;

        T calcRow = getRow(foundPartitionPos, foundRowPos);
        boolean rowMatchVersion = isRowMatchRowVersion(calcRow, version);
        if (!rowMatchVersion) {
            if (foundRowPos == 0) {
                if (foundPartitionPos == 0) {
                    return null;
                } else {
                    foundPartitionPos--;
                    foundRowPos = this.partitionCapacity - 1;

                    calcRow = getRow(foundPartitionPos, foundRowPos);
                    rowMatchVersion = isRowMatchRowVersion(calcRow, version);
                    if (!rowMatchVersion) {
                        return null;
                    }
                }
            } else {
                foundRowPos--;

                calcRow = getRow(foundPartitionPos, foundRowPos);
                rowMatchVersion = isRowMatchRowVersion(calcRow, version);
                if (!rowMatchVersion) {
                    return null;
                }
            }
        }

        Partition<K,T,V> partition = this.partitions.get(foundPartitionPos);
        return partition.get(foundRowPos);
    }

    public PartitionSearchRes<K,T,V> authenticatedSearch(V searchVersion) throws Exception {
        if (this.isEmpty()) {
            return null;
        }

        int posRes = this.getPosOfRowWithVersionEqualOrSmallerThan(searchVersion);

        int rowsToCopy = this.elemCount - posRes;
        int foundPartitionPos = posRes / this.partitionCapacity;
        int foundRowPos = posRes % this.partitionCapacity;

        T calcRow = getRow(foundPartitionPos, foundRowPos);
        boolean rowMatchVersion = isRowMatchRowVersion(calcRow, searchVersion);
        if (!rowMatchVersion) {
            if (foundRowPos == 0) {
                if (foundPartitionPos == 0) {
                    return null;
                } else {
                    foundPartitionPos--;
                    foundRowPos = this.partitionCapacity - 1;

                    calcRow = getRow(foundPartitionPos, foundRowPos);
                    rowMatchVersion = isRowMatchRowVersion(calcRow, searchVersion);
                    if (!rowMatchVersion) {
                        return null;
                    } else {
                        rowsToCopy++;
                    }
                }
            } else {
                foundRowPos--;

                calcRow = getRow(foundPartitionPos, foundRowPos);
                rowMatchVersion = isRowMatchRowVersion(calcRow, searchVersion);
                if (!rowMatchVersion) {
                    return null;
                } else {
                    rowsToCopy++;
                }
            }
        }

        byte[] prevRowDigest = getPrevRowDigest(foundPartitionPos, foundRowPos);

        ArrayList<T> res = null;
        if (rowsToCopy > 0) {
            res = new ArrayList<>(rowsToCopy);
            while (foundPartitionPos < this.partitions.size()) {
                Partition<K,T,V> partition = this.partitions.get(foundPartitionPos);
                while (foundRowPos < partition.size()) {
                    T row = partition.get(foundRowPos);
                    res.add(row);
                    foundRowPos++;
                    rowsToCopy--;
                }
                foundRowPos = 0;
                foundPartitionPos++;
            }
        }

//        Utils.assertTrue(rowsToCopy == 0, "should be true");
        return new PartitionSearchRes<>(prevRowDigest, res);
    }

//    public boolean isRowMatchPartitionRowVersion(int partitionPos, int rowPos, V searchVersion) throws Exception {
//        boolean rowMatchVersion;
//        T row = getRow(partitionPos, rowPos);
//        rowMatchVersion = isRowMatchRowVersion(row, searchVersion);
//        return rowMatchVersion;
//    }

    public byte[] getPrevRowDigest(int partitionPos, int rowPos) throws Exception {
        byte[] rowDigest;
        if (rowPos == 0) {
            if (partitionPos == 0) {
                rowDigest = Utils.nullDigest;
            } else {
                T row = getRow(partitionPos - 1, this.partitionCapacity - 1);
                rowDigest = row.getDigest();
            }
        } else {
            T row = getRow(partitionPos, rowPos - 1);
            rowDigest = row.getDigest();
        }

        return rowDigest;
    }

    private T getRow(int partitionPos, int rowPos) throws Exception {
//        Utils.assertTrue(partitionPos >= 0 && partitionPos < this.partitions.size(), "should be true");
        Partition<K, T, V> partition = this.partitions.get(partitionPos);
        return partition.get(rowPos);
    }

    public static <K extends Comparable<K>, T extends IRowDetails<K,T,V>, V extends Comparable<V>>
    boolean isRowMatchRowVersion(T row, V searchVersion) throws Exception {
        boolean rowMatchVersion;
        Version<V> rowVersion = row.getVersion();
        if (rowVersion.getValidFrom().compareTo(searchVersion) > 0) {
            rowMatchVersion = false;
        } else {  // rowValidFrom.compareTo(searchVersion) <= 0
            if (rowVersion.getValidTo() == null) {
                rowMatchVersion = true;
            } else if (rowVersion.getValidTo().compareTo(searchVersion) <= 0) {
                rowMatchVersion = false;
            } else {
                rowMatchVersion = true;
            }
        }
        return rowMatchVersion;
    }


//    public void modify(V currentVersion, T row) throws Exception {
//        T lastRow = getLastRow();
//        Version<V> lastRowVersion = lastRow.getVersion();
//        V lastRowCreateVersion = lastRowVersion.getCreateVersion();
//
//        if (row == null) {
//            deleteLastRow(currentVersion);
//        } else {
//            if (lastRowCreateVersion.compareTo(currentVersion) < 0) {
//                add(currentVersion, row);
//            } else if (lastRowCreateVersion.compareTo(currentVersion) == 0) {
//                updateLastRow(currentVersion, row);
//            } else {
//                throw new Exception("should not get here");
//            }
//        }
//    }

    public T deleteLastRow(V currentVersion) throws Exception {
//        Utils.assertTrue(!this.isEmpty(), "should be true");

        T deletedRow = deleteLastRowFromLastPartition(currentVersion);
        if (this.lastPartition.isEmpty()) {
            this.partitions.remove(this.partitions.size() - 1);
            if (this.isEmpty()) {
                this.lastPartition = null;
            } else {
                this.lastPartition = this.partitions.get(this.partitions.size() - 1);
            }
        }
        return deletedRow;
    }

    public T deleteLastRowFromLastPartition(V currentVersion) throws Exception {
        T deletedRow = null;
        Version<V> lastRowVersion = this.lastPartition.getLastRowVersion();

        int cmp = lastRowVersion.getValidFrom().compareTo(currentVersion);
        if (cmp == 0) {
            if (lastRowVersion.getValidTo() == null) {
                deletedRow = this.lastPartition.deleteLastRow();
                this.elemCount--;
            } else {
                throw new Exception("no row with the provided key to delete in the current version");
            }
        } else {
//            Utils.assertTrue(cmp < 0);
            if (lastRowVersion.getValidTo() == null) {
                lastRowVersion.setValidTo(currentVersion);

                byte[] prevRowDigest = getPrevRowDigest(this.partitions.size() - 1, this.lastPartition.size() - 1);
                T lastRow = this.lastPartition.getLastRow();
                lastRow.calculateDigest(prevRowDigest);
                deletedRow = lastRow;
            } else {
                throw new Exception("no row with the provided key to delete in the current version");
            }
        }
        return deletedRow;
    }

    public T updateLastRow(V currentVersion, T update) throws Exception {
        if (this.isEmpty()) {
            throw new Exception("no row with the provided key to update in the current version");
            //return null;
        }

//        V lastRowCreateVersion = lastRowVersion.getCreateVersion();
        //        V updateCreateVersion = updateVersion.getCreateVersion();

        if (update.getVersion().getValidTo() != null) {
            throw new Exception("updated rows should have null delete version");
        }

        Version<V> lastRowVersion = this.lastPartition.getLastRowVersion();
        if (lastRowVersion.getValidTo() != null) {
            throw new Exception("no row with the provided key to delete in the current version");
//            return null;
        }
        deleteLastRow(currentVersion);
        update = add(currentVersion, update);

        return update;

//        T prevRow = null;
//        byte[] prevRowDigest = null;
//        if (this.lastPartition.size() >= 2) {
//            prevRow = this.lastPartition.get(this.lastPartition.size() - 2);
//            prevRowDigest = prevRow.getDigest();
//        } else if (this.size() >= 2) {
//            Partition<K, T, V> prevPartition = this.partitions.get(this.size() - 2);
//            prevRow = prevPartition.get(this.partitionCapacity - 1);
//            prevRowDigest = prevRow.getDigest();
//        } else {
//            prevRowDigest = Utils.nullDigest;
//        }
//
//        update.calculateDigest(prevRowDigest);
//        this.lastPartition.updateLastRow(update);
    }

    public T getLastRow() throws Exception {
//        Utils.assertTrue(!this.isEmpty(), "should be true");

        return this.lastPartition.getLastRow();
    }

    public ArrayList<T> getAllRows() throws Exception {
        ArrayList<T> rows = new ArrayList<>();

        for (Partition<K, T, V> p : this.partitions) {
            for (int i=0;i<p.size();i++) {
                T r = p.get(i);
                rows.add(r);
            }
        }
        return rows;
    }

    @Override
    public String toString() {
        return partitions.toString();
    }
}
