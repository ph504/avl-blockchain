package approach4.temporal.AVL;

import approach4.IRowDetails;
import approach4.temporal.skipList.Tower;
import approach4.temporal.temporalPartitions.Partitions;

import java.util.ArrayList;

/**
 * a node in an avlTree (which is binary)
 * this node has pointers to left child
 * @param <VersionType>
 * @param <KeyType>
 * @param <BucketRowType>
 */
public class Node<VersionType extends Comparable<VersionType>,KeyType extends Comparable<KeyType>, BucketRowType extends IRowDetails<KeyType, BucketRowType, VersionType>> {
    public Node leftChild, rightChild, parent;
    public int height;

    protected byte[] digest;

    public final KeyType key;

    public Partitions<KeyType,BucketRowType,VersionType> value;

    public final int partitionCapacity;

    /*
    * @berief the constructor takes version and row
    * @param version is the date, VersionType is the type:Date
    * @param row is the version and key couple is type: TableRowIntDateCols extends IRowDetails
    * TODO: versioning to be added to the implementation, for now the schema is preserved for future versioning.
    * */
    public Node(VersionType version, BucketRowType row) throws Exception {
        // default height is 1.
        this(version, row, 1);
    }
    public Node(VersionType version, BucketRowType row, int partitionCapacity) throws Exception {

        /*the row is defined as the version,key couple and the get key retrieves first column*/
        this.key = row.getKey();

        this.partitionCapacity = partitionCapacity;
        value = new Partitions<>(this.partitionCapacity);
        if (row != null) {
            value.add(version, row);
        }

        /*the fields defualt values*/
        digest = null;
        leftChild = null;
        rightChild = null;
        parent = null;
        height = 1;
    }

    /*
    * should update digest, during insert, delete, and value change of self and offsprings*/
    public void updateDigest(){

    }

    public static void main(String[] args) {
        // TODO crteate a version and add it as a node
        // TODO create a row as well containing the nece values
//        Node head = new Node();
        System.out.println();
    }
}
