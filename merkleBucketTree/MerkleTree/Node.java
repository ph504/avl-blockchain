package merkleBucketTree.MerkleTree;

//Each node of the MBT is an instance of this class
public class Node {

    private Node left;
    private Node right;
    private byte[] digest;
    /*The largest bucketIndex of its children for a non leaf and the index of the 
    relevant bucket of the hash table for a leaf*/  
    private int bucketIndex;

    public Node(Node left, Node right, byte[] digest, int bucketIndex) {
        this.left = left;
        this.right = right;
        this.digest = digest;
        this.bucketIndex = bucketIndex;
    }

    public int getBucketIndex(){
    	return this.bucketIndex;
    }
    
    public void setBucketIndex(int bucketIndex) {
    	this.bucketIndex = bucketIndex;
    }
    
    public Node getLeft() {
        return left;
    }

    public void setLeft(Node left) {
        this.left = left;
    }

    public Node getRight() {
        return right;
    }

    public void setRight(Node right) {
        this.right = right;
    }

    public byte[] getDigest() throws Exception {
        if (this.digest == null) {
            throw new Exception("digest is null");
        }
        return this.digest;
    }

    public void setDigest(byte[] digest) {
        this.digest = digest;
    }
}