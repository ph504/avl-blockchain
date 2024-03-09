package approach4;

public class CompositeKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements Comparable<CompositeKey<K1, K2>> {

    public final K1 k1;
    public final K2 k2;

    public CompositeKey(K1 k1, K2 k2) {
        this.k1 = k1;
        this.k2 = k2;
    }


    @Override
    public int compareTo(CompositeKey<K1, K2> o) {
        if (this.k1 == null) {
            if (o.k1 == null) {
                if (this.k2 == null) {
                    if (o.k2 == null) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    if (o.k2 == null) {
                        return 1;
                    } else {
                        return this.k2.compareTo(o.k2);
                    }
                }
            } else {
                return -1;
            }
        } else {
            if (o.k1 == null) {
                return 1;
            } else {
                return this.k1.compareTo(o.k1);
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((this.k1 == null) ? 0 : this.k1.hashCode())
                + ((this.k2 == null) ? 0 : this.k2.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        return this.compareTo((CompositeKey<K1, K2>) obj) == 0;
    }

}
