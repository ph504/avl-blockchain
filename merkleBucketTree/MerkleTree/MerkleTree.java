package merkleBucketTree.MerkleTree;

import merkleBucketTree.HashTable.HashTable;
import merkleBucketTree.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;



public class MerkleTree<K extends Comparable<K>,V> {

//	private static int capacity = 5; // The capacity of the hash table
//	private static HashTable hashTable = new HashTable(capacity);

    public final HashTable<K,V> hashTable;

    public MerkleTree(int capacity) {
        hashTable = new HashTable<>(capacity);
    }

	/*Takes a list of buckets hashes and make a list of MBT nodes from them to 
	feed to buildTree method*/
    public Node generateTree(ArrayList<byte[]> blocksHashes) throws Exception {
		ArrayList<Node> childNodes = new ArrayList<>();
		
		for (byte[] message : blocksHashes)
			childNodes.add(new Node(null, null, message, -1)); // -1 is replaced with a correct bucket's index in the buildTree method.
		
		return buildTree(childNodes);
	}

	/*Takes a list of Nodes and converts them to leaves and then 
	constructs the higher levels of the MBT to its root*/
    private Node buildTree(ArrayList<Node> children) throws Exception {
        ArrayList<Node> parents = new ArrayList<>();

        boolean leafFlag = true; // To start working with leaf nodes.
        
        while (children.size() != 1) { // children.size() becomes 1 once the tree has been built. 
            int index = 0, length = children.size();
            
            while (index < length) {
                Node leftChild = children.get(index);
                if (leafFlag)
                	leftChild.setBucketIndex(index);
                
                Node rightChild = null;
                
                if ((index + 1) < length) {
                    rightChild = children.get(index + 1);
                    if (leafFlag)
                    	rightChild.setBucketIndex(index + 1);
                }
                else // To keep the tree balanced, left child is duplicated to create a right child when there is no more nodes.
                	rightChild = new Node(null, null, leftChild.getDigest(), -2);

                byte[] parentHash = getHash(leftChild.getDigest(), rightChild.getDigest());
//                String parentHash = HashAlgorithm.generateHash(leftChild.getHash() + rightChild.getHash());

                parents.add(new Node(leftChild, rightChild, parentHash, Math.max(leftChild.getBucketIndex(), rightChild.getBucketIndex())));
                index += 2;
            }
            children = parents;
            parents = new ArrayList<>();
            leafFlag = false; // To start working with non-leaf nodes.
        }
        return children.get(0);
    }

    private byte[] getHash(byte[] leftHash, byte[] rightHash) {
        byte[] digest = Utils.commutativeHash(leftHash, rightHash);
        return digest;
    }

    /*Shows the hashes of each level of MBT with an empty line to distinguish 
    consecutive levels*/
    public void printLevelOrderTraversal(Node root) throws Exception {
        if (root == null)
        	return;
        
        if ((root.getLeft() == null && root.getRight() == null))
        	System.out.format("Hash: '%s'--->Bucket's Index: '%d' \n", root.getDigest(), root.getBucketIndex());
        
        Queue<Node> queue = new LinkedList<>();
        queue.add(root);
        queue.add(null); // To put a delimiter between two consecutive levels of the MBT

        while (!queue.isEmpty()) {
            Node node = queue.poll();
            
            if (node != null)
            	System.out.format("Hash: '%s'--->Bucket's Index: '%d' \n", node.getDigest(),node.getBucketIndex());
            else
            {
            	System.out.println();
                if (!queue.isEmpty())
                	queue.add(null); // To put a delimiter between two consecutive levels of the MBT
            }

            if (node != null && node.getLeft() != null)
            	queue.add(node.getLeft());
            
            if (node != null && node.getRight() != null)
            	queue.add(node.getRight());
        }
    }
    
    /*Searches for the key in the MBT by traversing from its root to 
    the relevant bucket and then traversing the binary search tree in that bucket 
    considering copyOnWrite restriction*/
    public V copyOnWriteLookup(Node root, K key) throws Exception {
		if (root.getLeft() == null && root.getRight() == null)
			return hashTable.getValue(hashTable.buckets[root.getBucketIndex()], key);
		
		int bucketIndex = hashTable.bucketIndexFor(key);
    	
		if(bucketIndex <= root.getLeft().getBucketIndex())
			return copyOnWriteLookup(root.getLeft(), key);
		else
			return copyOnWriteLookup(root.getRight(), key);	
    }
    
    /*Inserts a node in the MBT and then produces the required new hashes in the MBT
    considering copyOnWrite restriction*/
    public void copyOnWriteInsert(Node root, K key, V value) throws Exception {
//		System.out.println("befor----"+ root.getHash() +" Bucketindex: " +root.getBucketIndex());
		
    	if (root.getLeft() == null && root.getRight() == null) {
//    		System.out.println("\nThe index of selected bucket to insert is : " + root.getBucketIndex());
    		hashTable.buckets[root.getBucketIndex()] = hashTable.insert(hashTable.buckets[root.getBucketIndex()], key, value);
//			System.out.format("Insert done!!! \nkey:'%s' and value:'%s' \n\n", key, value);
			byte[] newHash = hashTable.getHashOfBucket(hashTable.buckets[root.getBucketIndex()]);
			root.setDigest(newHash);
		}
		else 
		{
			int bucketIndex = hashTable.bucketIndexFor(key);
	    	if(bucketIndex <= root.getLeft().getBucketIndex()) {
				copyOnWriteInsert(root.getLeft(), key, value);

                byte[] parentHash = getHash(root.getLeft().getDigest(), root.getRight().getDigest());
//				String parentHash = HashAlgorithm.generateHash(root.getLeft().getHash() + root.getRight().getHash());
				root.setDigest(parentHash);
	    	}
			else {
				copyOnWriteInsert(root.getRight(), key, value);
                byte[] parentHash = getHash(root.getLeft().getDigest(), root.getRight().getDigest());
//				String parentHash = HashAlgorithm.generateHash(root.getLeft().getHash() + root.getRight().getHash());
				root.setDigest(parentHash);
			}
		}
    	
//    	System.out.println("after----"+ root.getHash() +" Bucketindex: " +root.getBucketIndex());
    }
    
    public static void main(String[] args) throws Exception {

        int capacity = 5;
        MerkleTree<String, String> merkleTree = new MerkleTree<>(capacity);
    	try {
    		File myObj = new File("input.txt");
    		Scanner myReader = new Scanner(myObj);
    		while (myReader.hasNextLine()) {
    			String line = myReader.nextLine();
    			String[] fields = line.split(",");
                merkleTree.hashTable.put(fields[0], fields[1]);
    		}
    		myReader.close();
    	} catch (FileNotFoundException e) {
    		System.out.println("An error occurred.");
    		e.printStackTrace();
    	}
    	
    	System.out.println("=============== Contents of hash table's buckets ===============");
        merkleTree.hashTable.showTable();
        
        System.out.println("========== Concatenated values of hash table's buckets =========");
        merkleTree.hashTable.showConcatenatedOfBucket();
        
        ArrayList<byte[]> blocksHashes = new ArrayList<>();
        merkleTree.hashTable.getHashes(blocksHashes);
        Node root = merkleTree.generateTree(blocksHashes);
        
        System.out.println("===================== Merkle tree's levels =====================");
        merkleTree.printLevelOrderTraversal(root);
        
        System.out.println("================== Lookup for an existing key ==================");
        String key = "Brian2";
        Object result = merkleTree.copyOnWriteLookup(root, key);
        if(result == null)
        	System.out.format("'%s' does not exist in the data. \n\n", key);
        else
        	System.out.format("'%s's value is : %s \n\n", key, result.toString());
        
        System.out.println("================= Lookup for a non-existing key =================");
        key = "Bahman";
        result = merkleTree.copyOnWriteLookup(root, key);
        if(result == null)
        	System.out.format("'%s' does not exist in the data. \n\n", key);
        else
        	System.out.format("'%s's value is : %s \n\n", key, result.toString());

        System.out.println("===================== Inserting a new node =====================");

        merkleTree.copyOnWriteInsert(root, "Bahman0", "000-0000");
        
        System.out.println("\n=================== New Merkle tree's levels ===================");
        merkleTree.printLevelOrderTraversal(root);
        
        System.out.println("=============== Contents of hash table's buckets ===============");
        merkleTree.hashTable.showTable();
    	
        System.out.println("============================= Bye! =============================");
    }
}
