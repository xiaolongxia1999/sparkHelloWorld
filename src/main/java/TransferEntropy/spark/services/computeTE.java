package TransferEntropy.spark.services;

/**
 * Created by Administrator on 2018/4/27 0027.
 */
public interface computeTE {
    public double computeDiscrete(int[] source,int[] dest);

    public double computeContinuous(double[] source,double[] dest);

//    public void setParams(String[] args);
}
