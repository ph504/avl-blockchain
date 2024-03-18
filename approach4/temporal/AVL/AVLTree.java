package approach4.temporal.AVL;

import approach4.IRowDetails;
import approach4.temporal.skipList.Tower;
import approach4.temporal.skipList.ToweredTypeUtils;

import java.util.Random;

public class AVLTree<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> {
    private final int partitionCapacity;
    private KVER currentVersion;
    //    private final double iterationProbability;
    // An instance of the random number generator.
    // TODO remove seed
    public static Random random = new Random();

    // no tail, as tail will be always null
    private final Node<KVER,K,V> head;

    //    private KVER currentVersion;
    AVLTree(KVER initVersion, int partitionCapacity) throws Exception{
        this.currentVersion = initVersion;
        this.partitionCapacity = partitionCapacity;
        this.head = new Node<>(initVersion, null)
    }
    insert(V row){

    }
}