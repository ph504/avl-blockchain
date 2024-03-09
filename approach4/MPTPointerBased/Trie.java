package approach4.MPTPointerBased;


import approach4.ITypeUtils;


public class Trie<K extends Comparable<K>,V> {
    public TrieNode<K,V> rootNode;
    private final Serializer<K> keySerializer;
    final private ITypeUtils<V> valueTypeUtils;

    public Trie(Serializer<K> keySerializer, ITypeUtils<V> valueTypeUtils) {
        this.keySerializer = keySerializer;
        this.valueTypeUtils = valueTypeUtils;
        this.rootNode = null;
    }

    public void put(K key, V value) throws Exception {
        this.rootNode = TrieNode.update(this.rootNode, keySerializer.serialize(key), value, this.valueTypeUtils);
    }

    public V get(K key) {
		return TrieNode.get(rootNode, keySerializer.serialize(key), this.valueTypeUtils);
	}








}
