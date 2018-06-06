package TransferEntropy.spark.services;

/**
 * Created by Administrator on 2018/6/5 0005.
 */
public interface compute<T> {

    public double process(T[] source,T[] dest);

    public void setParams(String[] args);
}
