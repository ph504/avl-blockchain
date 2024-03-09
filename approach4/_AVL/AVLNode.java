package approach4.AVL; /**
 * 
 */

/**
 * @author antonio081014
 * @time Jul 5, 2013, 9:31:32 PM
 */
public class AVLNode<K extends Comparable<K>, V> implements Comparable<AVLNode<K,V>> {

	private K key;
	private V value;
	private AVLNode<K,V> left;
	private AVLNode<K,V> right;
	public int level;
	private int depth;

	public AVLNode(K key, V value) {
		this(key, value, null, null);
	}

	public AVLNode(K key, V value, AVLNode<K,V> left, AVLNode<K,V> right) {
		super();
		this.key = key;
		this.value = value;
		this.left = left;
		this.right = right;
		if (left == null && right == null)
			setDepth(1);
		else if (left == null)
			setDepth(right.getDepth() + 1);
		else if (right == null)
			setDepth(left.getDepth() + 1);
		else
			setDepth(Math.max(left.getDepth(), right.getDepth()) + 1);
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public AVLNode<K,V> getLeft() {
		return left;
	}

	public void setLeft(AVLNode<K,V> left) {
		this.left = left;
	}

	public AVLNode<K,V> getRight() {
		return right;
	}

	public void setRight(AVLNode<K,V> right) {
		this.right = right;
	}

	/**
	 * @return the depth
	 */
	public int getDepth() {
		return depth;
	}

	/**
	 * @param depth
	 *            the depth to set
	 */
	public void setDepth(int depth) {
		this.depth = depth;
	}

	@Override
	public int compareTo(AVLNode<K,V> o) {
		return this.key.compareTo(o.key);
	}

	@Override
	public String toString() {
		return "Level " + level + ": " + key;
	}

	public V getValue() {
		return this.value;
	}
}
