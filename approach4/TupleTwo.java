package approach4;

public class TupleTwo<T1 , T2> {
    public T1 first;
    public T2 second;

    public TupleTwo(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return "first: " + this.first + " second: " + this.second;
    }
}
