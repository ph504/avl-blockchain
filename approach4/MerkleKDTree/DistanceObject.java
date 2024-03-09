package approach4.MerkleKDTree;

import java.util.Arrays;

public class DistanceObject {

    public final double[] point;
    public final double squareDistance;

    public DistanceObject(double[] point, double squareDistance) {
        this.point = Arrays.copyOf(point, point.length); // Create a copy of the point array
        this.squareDistance = squareDistance;
    }

}
