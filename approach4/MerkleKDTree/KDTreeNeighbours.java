package approach4.MerkleKDTree;

import java.util.ArrayList;
import java.util.List;

import static approach4.MerkleKDTree.Utils.squareDistance;

public class KDTreeNeighbours {
    private final double[] query_point;
    private final int t;
    public double largest_distance; // squared
    private final List<DistanceObject> current_best;

    public KDTreeNeighbours(double[] query_point, int t) {
        this.query_point = query_point;
        this.t = t;
        this.largest_distance = 0;
        this.current_best = new ArrayList<>();
    }

    public void calculateLargest() {
        if (t >= current_best.size()) {
            DistanceObject do_ = current_best.get(current_best.size() - 1);
            largest_distance = do_.squareDistance;
        } else {
            DistanceObject do_ = current_best.get(t - 1);
            largest_distance = do_.squareDistance;
        }
    }

    public void add(double[] point) {
        double sd = squareDistance(point, query_point);
        for (int i = 0; i < current_best.size(); i++) {
            if (i == t) {
                return;
            }

            DistanceObject do_ = current_best.get(i);

            if (do_.squareDistance > sd) {
                DistanceObject newDo = new DistanceObject(point, sd);
                current_best.add(i, newDo);
                calculateLargest();
                return;
            }
        }
        DistanceObject newDo = new DistanceObject(point, sd);
        current_best.add(newDo);
        calculateLargest();
    }

    public List<double[]> getBest() {
        List<double[]> bestList = new ArrayList<>();
        for (int i = 0; i < Math.min(t, current_best.size()); i++) {
            DistanceObject do_ = current_best.get(i);
            bestList.add(do_.point);
        }
        return bestList;
    }


}
