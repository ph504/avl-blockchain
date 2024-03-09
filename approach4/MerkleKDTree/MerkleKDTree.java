package approach4.MerkleKDTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static approach4.MerkleKDTree.Utils.combineAndHash;

public class MerkleKDTree {

    private final int dim;
    private final KDTreeNode root_node;

    public MerkleKDTree(double[][] data, int dim) {
        this.dim = dim;
        root_node = buildKDTree(data, 0);
    }

    public String computeMerkleRoot() {
        return leveledHash(root_node, 0);
    }

    private KDTreeNode buildKDTree(double[][] point_list, int depth) {
        if (point_list == null || point_list.length == 0) {
            return null;
        }

        int axis = depth % dim;

        Arrays.sort(point_list, Comparator.comparingDouble(point -> point[axis]));

        int median = point_list.length / 2;

        KDTreeNode node = new KDTreeNode(point_list[median],
                buildKDTree(Arrays.copyOfRange(point_list, 0, median), depth + 1),
                buildKDTree(Arrays.copyOfRange(point_list, median + 1, point_list.length), depth + 1));

        return node;
    }

    private String leveledHash(KDTreeNode node, int depth) {
        if (node == null) {
            return "";
        }

        String l_hash_pair = leveledHash(node.getLeft(), depth + 1);
        String r_hash_pair = leveledHash(node.getRight(), depth + 1);
        return combineAndHash(l_hash_pair, r_hash_pair);
    }



    public void range(double[] min_point, double[] max_point, List<Object> res) {
        rangeSearch(root_node, 0, min_point, max_point, res);
    }

    private void rangeSearch(KDTreeNode node, int depth, double[] min_point, double[] max_point, List<Object> res) {
        if (node == null) {
            return;
        }

        double min_dist = leveledDistance(node, min_point, depth);
        if (min_dist <= 0) {
            rangeSearch(node.getLeft(), depth + 1, min_point, max_point, res);
        }

        double max_dist = leveledDistance(node, max_point, depth);
        if (max_dist >= 0) {
            rangeSearch(node.getRight(), depth + 1, min_point, max_point, res);
        }

        for (int i = 0; i < this.dim; i++) {
            if (node.getPoint()[i] < min_point[i] || node.getPoint()[i] > max_point[i]) {
                return;
            }
        }

        res.add(node.getPoint());
    }

    private double leveledDistance(KDTreeNode node, double[] point, int depth) {
        int axis = depth % dim;
        return point[axis] - node.getPoint()[axis];
    }

    public List<double[]> query(double[] query_point, int count_nn) {
        KDTreeNeighbours neighbours = new KDTreeNeighbours(query_point, count_nn);
        nearestNeighbourSearch(root_node, query_point, count_nn, 0, neighbours);
        return neighbours.getBest();
    }

    private void nearestNeighbourSearch(KDTreeNode node, double[] query_point, int count_nn, int depth, KDTreeNeighbours best_neighbours) {
        if (node == null) {
            return;
        }

        if (node.isLeaf()) {
            best_neighbours.add(node.getPoint());
            return;
        }

        int axis = depth % dim;
        KDTreeNode near_subtree;
        KDTreeNode far_subtree;

        if (query_point[axis] < node.getPoint()[axis]) {
            near_subtree = node.getLeft();
            far_subtree = node.getRight();
        } else {
            near_subtree = node.getRight();
            far_subtree = node.getLeft();
        }

        nearestNeighbourSearch(near_subtree, query_point, count_nn, depth + 1, best_neighbours);

        best_neighbours.add(node.getPoint());


        double axis_distance = Math.pow((node.getPoint()[axis] - query_point[axis]), 2);
        if (axis_distance < best_neighbours.largest_distance) {
            nearestNeighbourSearch(far_subtree, query_point, count_nn, depth + 1, best_neighbours);
        }
    }

    public static MerkleKDTree constructFromData(double[][] data, int dim) {
        return new MerkleKDTree(data, dim);
    }

    public static void main(String[] args) {
        double[][] data = {
                {1, 2, 3, 1234},
                {2, 3, 4, 1234},
                {1, 0, 2, 1234},
                {1, 4, 3, 1234},
                {1, 3, 3, 1234},
                {1, 3, 5, 1234},
                {4, 5, 1, 1234},
                {2, 4, 0, 1234},
                {5, 1, 2, 1234},
                {4, 5, 4, 1234},
                {4, 3, 1, 1234}
        };

        test1(data);
    }

    public static void test1(double[][] data) {
        MerkleKDTree tree = MerkleKDTree.constructFromData(data, 2);
        System.out.println(tree);

//        double[] min_point = {1, 2, 2};
//        double[] max_point = {1, 3, 4};

//        double[] min_point = {1, 0, 0};
//        double[] max_point = {1, 100, 100};

        double[] min_point = {1, 0, 2};
        double[] max_point = {1, 100, 4};

//        double[] min_point = {0, 0, 0};
//        double[] max_point = {100, 100, 100};

        List<Object> in_range_points = new ArrayList<>();
        tree.range(min_point, max_point, in_range_points);

        System.out.println(in_range_points);
        System.out.println();
        System.out.println(tree.computeMerkleRoot());
    }

//    public static void test2(double[][] data) {
//        KDTreeNode k = new KDTreeNode(data[0], null, null);
//        System.out.println(k.getPointHash());
//    }

    public static void test3(double[][] data) {
        MerkleKDTree tree = MerkleKDTree.constructFromData(data, 3);
        System.out.println(tree);

        double[] q_point = {4, 4, 4};
        int count_nn = 5;
        List<double[]> knn_points = tree.query(q_point, count_nn);
        System.out.println(knn_points);
    }
}
