package approach4.temporal.skipList;

import approach4.IRowDetails;

public class StackTuple<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> {
    public final int level;
    public final Tower<KVER,K,V> tower;
    public Tower<KVER,K,V> pointedTower;
    public boolean performSetOperation;


    public StackTuple(int level, Tower<KVER, K, V> tower, Tower<KVER, K, V> pointedTower, boolean performSetOperation) {
        this.level = level;
        this.tower = tower;
        this.pointedTower = pointedTower;
        this.performSetOperation = performSetOperation;

    }

    @Override
    public String toString() {
        return "level: " + this.level + " tower: (" + this.tower + ")";
    }
}
