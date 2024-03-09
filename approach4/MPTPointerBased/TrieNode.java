package approach4.MPTPointerBased;

import approach4.ITypeUtils;
import approach4.Utils;

import static approach4.MPTPointerBased.NibbleString.*;


public class TrieNode<K extends Comparable<K>,V> {
    private byte[] digest;
    public Object[] data;
    final public NodeType type;

    public TrieNode(NodeType type) throws Exception {
        this.type = type;
        this.digest = null;

        if (type == NodeType.BRANCH) {
            this.data = new Object[17];
        } else if (type == NodeType.LEAF) {
            this.data = new Object[2];
        } else if (type == NodeType.EXTENSION) {
            this.data = new Object[2];
        } else {
            throw new Exception("should not get here");
        }
    }

    public enum NodeType {
        BRANCH,
        LEAF,
        EXTENSION
    }


    public void calculateDigest(ITypeUtils<V> valueTypeUtils) throws Exception {

        if (type == NodeType.BRANCH) {
            TrieNode<K,V> node = (TrieNode<K, V>) this.data[0];
            this.digest = Utils.nullDigest;
            if (node != null) {
                this.digest = node.getDigest();
            }

            for (int i=1; i< 16; i++) {
                node = (TrieNode<K, V>) this.data[i];
                byte[] nodeDigest = Utils.nullDigest;
                if (node != null) {
                    nodeDigest = node.getDigest();
                }

                this.digest = Utils.getHash(this.digest, nodeDigest);
            }
            V val = (V) this.data[16];
            byte[] valDigest = Utils.nullDigest;
            if (val != null) {
                valDigest = valueTypeUtils.getZeroLevelDigest(val);
            }
            this.digest = Utils.getHash(this.digest, valDigest);
        } else if (type == NodeType.LEAF) {
            this.digest = Utils.hash((byte[]) this.data[0]);
            V val = (V) this.data[1];
            this.digest = Utils.getHash(this.digest, valueTypeUtils.getZeroLevelDigest(val));
        } else if (type == NodeType.EXTENSION) {
            this.digest = Utils.hash((byte[]) this.data[0]);
            TrieNode<K,V> node = (TrieNode<K, V>) this.data[1];
            this.digest = Utils.getHash(this.digest, node.getDigest());
        } else {
            throw new Exception("should not get here");
        }
    }

    public byte[] getDigest() throws Exception {
        if (this.digest == null) {
            throw new Exception("digest is null");
        }
        return this.digest;
    }

    static public<K extends Comparable<K>,V> V get(TrieNode<K,V> rootNode, byte[] key, ITypeUtils<V> valueTypeUtils) {
        return getHelper(rootNode, from(key), valueTypeUtils);
    }

    static public<K extends Comparable<K>,V> V getHelper(TrieNode<K,V> node, NibbleString path, ITypeUtils<V> valueTypeUtils) {

		if (node == null) {
            return null;
        }

        NodeType type = node.type;

		if (type == NodeType.BRANCH) {
			if (path.size() == 0) {
                return (V) node.data[16];
            }

			TrieNode<K,V> subNode = (TrieNode<K, V>) node.data[path.nibbleAsByte(0)];
			return getHelper(subNode, path.substring(1), valueTypeUtils);
		}

		NibbleString key = unpack((byte[]) node.data[0]);
		if (type == NodeType.LEAF) {
			return path.equals(key) ? (V) node.data[1] : null;
		}

		if (type == NodeType.EXTENSION) {
			if (key.equals(path.substring(0, key.size()))) {
                return getHelper((TrieNode<K, V>) node.data[1], path.substring(key.size()), valueTypeUtils);
            } else {
                return null;
            }
		}

		throw new AssertionError("Not possible");
	}

    static public<K extends Comparable<K>,V> TrieNode<K,V> update(TrieNode<K,V> rootNode, byte[] key, V value, ITypeUtils<V> valueTypeUtils) throws Exception {
        TrieNode<K, V> node = updateHelper(rootNode, from(key), value, valueTypeUtils);
        return node;
    }

    static private<K extends Comparable<K>,V> TrieNode<K,V> updateHelper(TrieNode<K,V> node, NibbleString path, V value, ITypeUtils<V> valueTypeUtils) throws Exception {
        if (node == null) {
            TrieNode<K, V> newNode = new TrieNode<>(NodeType.LEAF);
            newNode.data[0] = pack(path, true);
            newNode.data[1] = value;
            newNode.calculateDigest(valueTypeUtils);
            return newNode;
        }

        NodeType type = node.type;
        if (type == NodeType.BRANCH) {
            if (path.size() == 0) {
                node.data[16] = value;
            } else {
                int keyIndex = path.nibbleAsByte(0);
                TrieNode<K,V> newNode = (TrieNode<K, V>) node.data[keyIndex];
                newNode = updateHelper(newNode, path.substring(1), value, valueTypeUtils);
                node.data[keyIndex] = newNode;
            }
            node.calculateDigest(valueTypeUtils);
            return node;

        } else if (type == NodeType.LEAF || type == NodeType.EXTENSION) {
            return updateKeyValueHelper(node, path, value, valueTypeUtils);
        }

        throw new AssertionError("Not possible");
    }

    static private<K extends Comparable<K>,V> TrieNode<K,V> updateKeyValueHelper(TrieNode<K,V> node, NibbleString path, V value, ITypeUtils<V> valueTypeUtils) throws Exception {
		NodeType type = node.type;
		NibbleString key = unpack((byte[]) node.data[0]);

		int prefixLength = getLongestCommonPrefixLength(path, key);

		NibbleString remainingPath = path.substring(prefixLength);
		NibbleString remainingKey = key.substring(prefixLength);

		TrieNode<K,V> newNode;
		if (remainingPath.size() == 0 && remainingKey.size() == 0) {
			if (type == NodeType.LEAF) {
			    node.data[1] = value;
                node.calculateDigest(valueTypeUtils);
			    return node;
			} else {
				newNode = updateHelper((TrieNode<K,V>)node.data[1], remainingPath, value, valueTypeUtils);
			}


		} else if (remainingKey.size() == 0) {
			if (type == NodeType.EXTENSION) {
				newNode = updateHelper((TrieNode<K,V>)node.data[1], remainingPath, value, valueTypeUtils);
			} else {
                TrieNode<K,V> leaf = new TrieNode<>(NodeType.LEAF);
                leaf.data[0] = pack(remainingPath.substring(1), true);
                leaf.data[1] = value;
                leaf.calculateDigest(valueTypeUtils);

                newNode = new TrieNode<>(NodeType.BRANCH);
                newNode.data[remainingPath.nibbleAsByte(0)] = leaf;
                newNode.data[16] = node.data[1];
                newNode.calculateDigest(valueTypeUtils);
			}

		} else {
            newNode = new TrieNode<>(NodeType.BRANCH);

			if (remainingKey.size() == 1 && type == NodeType.EXTENSION) {
			    newNode.data[remainingKey.nibbleAsByte(0)] = node.data[1];
                newNode.calculateDigest(valueTypeUtils);
			} else {
				byte[] packedChildKey = pack(remainingKey.substring(1), type == NodeType.LEAF);

                TrieNode<K,V> child;
                if (type == NodeType.LEAF) {
                    child = new TrieNode<>(NodeType.LEAF);
                } else {
                    child = new TrieNode<>(NodeType.EXTENSION);
                }
                child.data[0] = packedChildKey;
                child.data[1] = node.data[1];
                child.calculateDigest(valueTypeUtils);

                newNode.data[remainingKey.nibbleAsByte(0)] = child;
                newNode.calculateDigest(valueTypeUtils);
			}

			if (remainingPath.size() == 0) {
			    newNode.data[16] = value;
                newNode.calculateDigest(valueTypeUtils);
			} else {
				byte[] packedRemainingPath = pack(remainingPath.substring(1), true);

				TrieNode<K,V> leaf = new TrieNode<>(NodeType.LEAF);
				leaf.data[0] = packedRemainingPath;
                leaf.data[1] = value;
                leaf.calculateDigest(valueTypeUtils);

				newNode.data[remainingPath.nibbleAsByte(0)] = leaf;
                newNode.calculateDigest(valueTypeUtils);
			}
		}

		if (prefixLength > 0) {
		    TrieNode<K,V> n = new TrieNode<>(NodeType.EXTENSION);
		    n.data[0] = pack(key.substring(0, prefixLength), false);
		    n.data[1] = newNode;
            n.calculateDigest(valueTypeUtils);
			return n;
		} else {
			return newNode;
		}
	}

	private static int getLongestCommonPrefixLength(NibbleString path, NibbleString key) {
		int minKeyLength = Math.min(key.size(), path.size());
		int i=0;
		while (i < minKeyLength && key.nibbleAsChar(i) == path.nibbleAsChar(i)) i++;
		int prefixLength = i;
		return prefixLength;
	}


}
