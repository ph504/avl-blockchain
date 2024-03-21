package approach4;

public interface ITypeUtils<T> {
    //ihnfi
    byte[] getZeroLevelDigest(T obj) throws Exception;
    T clone(T obj) throws Exception;
}
