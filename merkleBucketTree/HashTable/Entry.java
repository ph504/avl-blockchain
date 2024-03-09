package merkleBucketTree.HashTable;


import merkleBucketTree.Utils;

//Each entry stores a (key, value) pair and two references to its children */
public class  Entry<K extends Comparable<K>,V> {
	private final K key;
	private V value;
	private Entry<K,V> left, right;
	private byte[] digest;

	//Create new entry.
	public Entry(K k, V v) throws Exception {
		this.key = k;
		this.value = v;
		this.left = null;
		this.right = null;
		calcDigest();

	}

	public void calcDigest() throws Exception {
		byte[] keyDigest = Utils.getNullableDigest(this.key);
		byte[] valDigest = Utils.getNullableDigest(this.value);
		byte[] keyValDigest = Utils.commutativeHash(keyDigest, valDigest);

		byte[] leftDigest;
		if (this.left == null) {
			leftDigest = Utils.nullDigest;
		} else {
			leftDigest = this.left.getDigest();
		}
		byte[] rightDigest;
		if (this.right == null) {
			rightDigest = Utils.nullDigest;
		} else {
			rightDigest = this.right.getDigest();
		}
		byte[] leftRightDigest = Utils.commutativeHash(leftDigest, rightDigest);
		this.digest = Utils.commutativeHash(keyValDigest, leftRightDigest);
	}

	public K getKey() {
		return this.key;
	}
	
	public V getValue() {
		return this.value;
	}
	public void setValue(V value) throws Exception {
		this.value = value;
		calcDigest();
	}
	
	public Entry<K,V> getLeft() {
		return this.left;
	}
	public void setLeft(Entry<K,V> node) throws Exception {
		this.left = node;
		calcDigest();
	}
	
	public Entry<K,V> getRight() {
		return this.right;
	}
	public void setRight(Entry<K,V> node) throws Exception {
		this.right= node;
		calcDigest();
	}


	public byte[] getDigest() throws Exception {
		if (this.digest == null) {
			throw new Exception("digest is null");
		}
		return this.digest;
	}
}