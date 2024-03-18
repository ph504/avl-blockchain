package approach4.temporal.AVL;

import approach4.IRowDetails;
import approach4.temporal.skipList.Tower;
import approach4.temporal.temporalPartitions.Partitions;

import java.util.ArrayList;

public class Node<KVER extends Comparable<KVER>,K extends Comparable<K>, V extends IRowDetails<K, V, KVER>> {
    public Node leftChild, rightChild, parent;

    public int height;

    protected byte[] digest;

    public final K key;

    public Partitions<K,V,KVER> value;

    public final int partitionCapacity;

    public Node(KVER version, V row) throws Exception {
        this(version, row, 1);
    }
    public Node(KVER version, V row, int partitionCapacity) throws Exception {

        this.key = row.getKey();

        this.partitionCapacity = partitionCapacity;
        value = new Partitions<>(this.partitionCapacity);
        if (row != null) {
            value.add(version, row);
        }

        digest = null;
        leftChild = null;
        rightChild = null;
        parent = null;
        height = 1;
    }

    public static void main(String[] args) {
        Node head = new Node();
        System.out.println();
    }
}
