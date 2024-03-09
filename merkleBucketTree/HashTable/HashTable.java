package merkleBucketTree.HashTable;

import merkleBucketTree.Utils;

import java.util.ArrayList;



public class HashTable<K extends Comparable<K>,V> {
	private final int capacity;
	public Entry<K,V>[] buckets;
	byte[] digest = null;
	
	public HashTable(int capacity) {
		if (capacity <= 0)
			throw new IllegalArgumentException(
					"Capacity is negative.");
		
		this.capacity = capacity;
		this.buckets = (Entry<K, V>[]) new Entry[capacity];
	}
	
	//Returns index for key based on its hash code.	
	public int bucketIndexFor(K key) throws Exception {
//		byte[] keyDigest = Utils.getNullableDigest(key);
//		int h = keyDigest[28] << 24 | (keyDigest[29] & 0xFF) << 16 | (keyDigest[30] & 0xFF) << 8 | (keyDigest[31] & 0xFF);
		int h = key.hashCode() & 0x7fffffff;
		return h % this.capacity;
	}

	public byte[] getDigest() throws Exception {
		if (this.digest == null) {
			throw new Exception("digest is null");
		}
		return this.digest;
	}
	
	/*
	Associates the specified value with the specified key in this bucket of the hash table.
	If bucket previously contained a mapping for this key, the old value is replaced.
	*/
	public Entry<K,V> insert(Entry<K,V> node, K key, V value) throws Exception {
		if (node == null) {
			node = new Entry<>(key, value);
			//System.out.format("Inserted key:'%s' and value:'%s' \n", node.getKey(), node.value);
		}
		else
			{
			if (key.compareTo(node.getKey()) == 0) {
				node.setValue(value);
			}
			else if (key.compareTo(node.getKey())< 0) {
				Entry<K,V> temp = insert(node.getLeft(), key, value);
				node.setLeft(temp);
				//System.out.format("\t insert into left child: %s:%s \n", key, value);
			}
			else
				{
					Entry<K,V> temp = insert(node.getRight(), key, value );
				node.setRight(temp);
				//System.out.format("\t insert into right child: %s:%s \n", key, value);
			}
		}
		return node;
	}
	
	public void put(K key, V value) throws Exception {
		int i = bucketIndexFor(key);
		
		if (this.buckets[i] == null) {
			this.buckets[i] = new Entry<>(key, value);
			//System.out.format("Set the root of bucket %d  with '%s':'%s' \n", i, key, value );
		}
		else {
			//System.out.format("put into bucket %d :\n", i );
			insert(this.buckets[i], key, value );
		}
	}
	  
	/*Returns the value to which the specified key is mapped in this bucket of the hash table,
	or null if the current bucket contains no mapping for this key.
	*/
	public V getValue(Entry<K,V> node, K key)
	{
		if (node == null)
			return null;
		else
			{
			if ((key.compareTo(node.getKey()) == 0))
				return node.getValue();
			else if (key.compareTo(node.getKey())< 0)
				return getValue(node.getLeft(), key);
			else
				return getValue(node.getRight(), key);
			}
	}
	
	public void getHashes(ArrayList<byte[]> blocksHashes) throws Exception {
		for (int j = 0; j < this.capacity; j++) {
			byte[] hash = getHashOfBucket(this.buckets[j]);
			blocksHashes.add(hash);
		}
	}
	
	public byte[] getHashOfBucket(Entry<K,V> node) throws Exception {
		byte[] digest;
		if (node == null) {
			digest = Utils.nullDigest;
		} else {
			digest = node.getDigest();
		}

		return digest;
	}


	

	
	public void showConcatenatedOfBucket() throws Exception {
		for (int j = 0; j < this.capacity; j++) {
			System.out.println("Bucket " + j + " :");
			byte[] sum = getHashOfBucket(this.buckets[j]);
			System.out.format("\t Concatenated : '%s' \n", sum);
			System.out.println();
		}
	}
	
	public void showTable() {
		for (int j = 0; j < this.capacity; j++) {
			System.out.println("Bucket " + j + " :");
			inorder(this.buckets[j]);
			System.out.println();
		}
	}
	
	private void inorder(Entry<K,V> r) {
		if (r != null) {
			inorder(r.getLeft());
			System.out.println("\t (" + r.getKey() + ", " + r.getValue() + ")");
			inorder(r.getRight());
		}
	}
}

