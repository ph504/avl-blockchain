package approach4.MerkleKDTree;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static approach4.MerkleKDTree.Utils.sha256Hash;

public class KDTreeNode {

    private String point_hash;
    private final double[] point;
    private KDTreeNode left;
    private KDTreeNode right;

    public KDTreeNode(double[] point, KDTreeNode left, KDTreeNode right) {
        this.point_hash = sha256Hash(point);
        this.point =  Arrays.copyOf(point, point.length);
        this.left = left;
        this.right = right;
    }

    public boolean isLeaf() {
        return (left == null && right == null);
    }

    public double[] getPoint() {
        return point;
    }

    public KDTreeNode getLeft() {
        return left;
    }

    public KDTreeNode getRight() {
        return right;
    }



    // Your other methods or functionalities for the KDTreeNode class can be added here.
}







