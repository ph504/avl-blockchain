package approach4.temporal.AVL;

import approach4.IRowDetails;
import approach4.temporal.skipList.Tower;
import approach4.temporal.skipList.ToweredTypeUtils;

import java.util.Random;

public class AVLTree<VersionType extends Comparable<VersionType>,KeyType extends Comparable<KeyType>,BucketRowType extends IRowDetails<KeyType,BucketRowType,VersionType>> {
    private final int partitionCapacity;
    private VersionType currentVersion;
    //    private final double iterationProbability;
    // An instance of the random number generator.
    // TODO remove seed
    public static Random random = new Random();

    // no tail, as tail will be always null
    private final Node<VersionType,KeyType,BucketRowType> head;

    //    private VersionType currentVersion;
    public AVLTree(VersionType initVersion, int partitionCapacity) throws Exception{
        this.currentVersion = initVersion;
        this.partitionCapacity = partitionCapacity;
        this.head = new Node<>(initVersion, null);
    }

    // TODO add versioning
    // note for versioning: we don't have to add this toweredtypeutils we can call whenever we want to add it with the digest calls.
    public AVLTree(
                VersionType initVersion,
                int partitionCapacity,
                ToweredTypeUtils<KeyType, BucketRowType> toweredTypeUtils)
            throws Exception{
        this.currentVersion = initVersion;
        this.partitionCapacity = partitionCapacity;
        this.head = new Node<>(initVersion, null);
    }
    public void insert(KeyType key, 
                       BucketRowType row, 
                       Node<VersionType, KeyType, BucketRowType> currentNode) 
            throws Exception{
        //head should never be null tho?
//        if (head==null)
//                throw new Exception("head should not be null");
        key = row.getKey();
        if (key.compareTo(currentNode.key)<0)
            if (currentNode.leftChild == null){
                currentNode.leftChild =
                        new Node<VersionType, KeyType, BucketRowType>(currentVersion, row);
                
                // set parent
                currentNode.leftChild.parent = currentNode;
                //        self._inspect_insertion(currentNode.leftChild)
            }
            else insert(key, row, currentNode.leftChild);
        
        else if (key.compareTo(currentNode.key)>0)
            if (currentNode.rightChild == null) {
                currentNode.rightChild =
                        new Node<VersionType, KeyType, BucketRowType>(currentVersion, row);
                // set parent
                currentNode.rightChild.parent = currentNode;
    //        self._inspect_insertion(currentNode.rightChild)
            }
            else insert(key, row, currentNode.rightChild);

        else System.out.println("Value already in tree!");

    }

    // demo tests
    public static void main(String[] args) {
        // create a tree with nodes, see if the insert and all operations works as intended

        System.out.println();
    }
}
