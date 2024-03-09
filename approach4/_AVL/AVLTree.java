package approach4.AVL;
import java.util.LinkedList;
import java.util.Queue;

public class AVLTree<K extends Comparable<K>,V> {
    AVLNode<K,V> root;

    public AVLTree() {
        root = null;
    }

    public K Maximum() {
        AVLNode<K,V> local = root;
        if (local == null)
            return null;
        while (local.getRight() != null)
            local = local.getRight();
        return local.getKey();
    }

    public K Minimum() {
        AVLNode<K,V> local = root;
        if (local == null)
            return null;
        while (local.getLeft() != null) {
            local = local.getLeft();
        }
        return local.getKey();
    }

    private int depth(AVLNode<K,V> node) {
        if (node == null)
            return 0;
        return node.getDepth();
        // 1 + Math.max(depth(node.getLeft()), depth(node.getRight()));
    }

    public AVLNode<K,V> insert(K key, V value) {
        root = insert(root, key, value);
        switch (balanceNumber(root)) {
            case 1:
                root = rotateLeft(root);
                break;
            case -1:
                root = rotateRight(root);
                break;
            default:
                break;
        }
        return root;
    }

    public AVLNode<K,V> insert(AVLNode<K,V> node, K key, V value) {
        if (node == null)
            return new AVLNode<K,V>(key, value);
        if (node.getKey().compareTo(key) > 0) {
            node = new AVLNode<K,V>(node.getKey(), node.getValue(), insert(node.getLeft(), key, value),
                    node.getRight());
            // node.setLeft(insert(node.getLeft(), key));
        } else if (node.getKey().compareTo(key) < 0) {
            // node.setRight(insert(node.getRight(), key));
            node = new AVLNode<K,V>(node.getKey(), node.getValue(), node.getLeft(), insert(
                    node.getRight(), key, value));
        }
        // After insert the new node, check and rebalance the current node if
        // necessary.
        switch (balanceNumber(node)) {
            case 1:
                node = rotateLeft(node);
                break;
            case -1:
                node = rotateRight(node);
                break;
            default:
                return node;
        }
        return node;
    }

    private int balanceNumber(AVLNode<K,V> node) {
        int L = depth(node.getLeft());
        int R = depth(node.getRight());
        if (L - R >= 2)
            return -1;
        else if (L - R <= -2)
            return 1;
        return 0;
    }

    private AVLNode<K,V> rotateLeft(AVLNode<K,V> node) {
        AVLNode<K,V> q = node;
        AVLNode<K,V> p = q.getRight();
        AVLNode<K,V> c = q.getLeft();
        AVLNode<K,V> a = p.getLeft();
        AVLNode<K,V> b = p.getRight();
        q = new AVLNode<K,V>(q.getKey(), q.getValue(), c, a);
        p = new AVLNode<K,V>(p.getKey(), p.getValue(), q, b);
        return p;
    }

    private AVLNode<K,V> rotateRight(AVLNode<K,V> node) {
        AVLNode<K,V> q = node;
        AVLNode<K,V> p = q.getLeft();
        AVLNode<K,V> c = q.getRight();
        AVLNode<K,V> a = p.getLeft();
        AVLNode<K,V> b = p.getRight();
        q = new AVLNode<K,V>(q.getKey(), q.getValue(), b, c);
        p = new AVLNode<K,V>(p.getKey(), p.getValue(), a, q);
        return p;
    }

    public V search(K key) {
        AVLNode<K,V> local = root;
        while (local != null) {
            if (local.getKey().compareTo(key) == 0)
                return local.getValue();
            else if (local.getKey().compareTo(key) > 0)
                local = local.getLeft();
            else
                local = local.getRight();
        }
        return null;
    }



    public Iterable<V> values(K lo, K hi) {
        if (lo == null) throw new IllegalArgumentException("first argument to keys() is null");
        if (hi == null) throw new IllegalArgumentException("second argument to keys() is null");

        Queue<V> queue = new LinkedList<V>();
        values(root, queue, lo, hi);
        return queue;
    }

    private void values(AVLNode<K,V> x, Queue<V> queue, K lo, K hi) {
        if (x == null) return;
        int cmplo = lo.compareTo(x.getKey());
        int cmphi = hi.compareTo(x.getKey());
        if (cmplo < 0) values(x.getLeft(), queue, lo, hi);
        if (cmplo <= 0 && cmphi >= 0) queue.add(x.getValue());
        if (cmphi > 0) values(x.getRight(), queue, lo, hi);
    }

    public String toString() {
        return root.toString();
    }

    public void PrintTree() {
        root.level = 0;
        Queue<AVLNode<K,V>> queue = new LinkedList<AVLNode<K,V>>();
        queue.add(root);
        while (!queue.isEmpty()) {
            AVLNode<K,V> node = queue.poll();
            System.out.println(node);
            int level = node.level;
            AVLNode<K,V> left = node.getLeft();
            AVLNode<K,V> right = node.getRight();
            if (left != null) {
                left.level = level + 1;
                queue.add(left);
            }
            if (right != null) {
                right.level = level + 1;
                queue.add(right);
            }
        }
    }
}