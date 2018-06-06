package TransferEntropy.spark.servicesImpl;

import TransferEntropy.spark.services.computeTE;
import infodynamics.measures.continuous.TransferEntropyCalculator;
import infodynamics.measures.continuous.gaussian.TransferEntropyCalculatorGaussian;
import infodynamics.utils.EmpiricalMeasurementDistribution;

import static java.lang.Thread.sleep;

/**
 * Created by Administrator on 2018/4/27 0027.
 */
public class GaussianTE implements computeTE {
    private int k;
    private int k_tau;
    private int l;
    private int l_tau;
    private int delay;
    public TransferEntropyCalculatorGaussian calc = new TransferEntropyCalculatorGaussian();


    public GaussianTE() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    }

    public GaussianTE(int k, int k_tau, int l, int l_tau, int delay) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.k = k;
        this.k_tau = k_tau;
        this.l = l;
        this.l_tau = l_tau;
        this.delay = delay;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getK_tau() {
        return k_tau;
    }

    public void setK_tau(int k_tau) {
        this.k_tau = k_tau;
    }

    public int getL() {
        return l;
    }

    public void setL(int l) {
        this.l = l;
    }

    public int getL_tau() {
        return l_tau;
    }

    public void setL_tau(int l_tau) {
        this.l_tau = l_tau;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public void setParams(int k, int k_tau, int l, int l_tau, int delay) throws Exception {
        calc.setProperty(TransferEntropyCalculator.K_PROP_NAME, String.valueOf(k));
        calc.setProperty(TransferEntropyCalculator.K_TAU_PROP_NAME, String.valueOf(k_tau));
        calc.setProperty(TransferEntropyCalculator.L_PROP_NAME, String.valueOf(l));
        calc.setProperty(TransferEntropyCalculator.L_TAU_PROP_NAME, String.valueOf(l_tau));
        calc.setProperty(TransferEntropyCalculator.DELAY_PROP_NAME, String.valueOf(delay));
    }

    public void setParams() throws Exception {
//       K_PROP_NAME
//       K_TAU_PROP_NAME
//       L_PROP_NAME
//       L_TAU_PROP_NAME
//       DELAY_PROP_NAME
        calc.setProperty(TransferEntropyCalculator.K_PROP_NAME, String.valueOf(this.getK()));
        calc.setProperty(TransferEntropyCalculator.K_TAU_PROP_NAME, String.valueOf(this.getK_tau()));
        calc.setProperty(TransferEntropyCalculator.L_PROP_NAME, String.valueOf(this.getL()));
        calc.setProperty(TransferEntropyCalculator.L_TAU_PROP_NAME, String.valueOf(this.getL_tau()));
        calc.setProperty(TransferEntropyCalculator.DELAY_PROP_NAME, String.valueOf(this.getDelay()));
    }
//这里setParams无效，改用下面的，直接传k,l,delay，用initialise传
//    public double  process(double[] source,double[] dest) throws Exception {
//        this.setParams();
////        calc.setProperty(TransferEntropyCalculator.K_PROP_NAME, String.valueOf(getK()));
//        calc.initialise();
//        calc.setObservations(source, dest);
//        double result = calc.computeAverageLocalOfObservations();           //从这里开始报错，很多传递熵根本没有计算。
//        return result;
//    }


    public double  process(double[] source,double[] dest) throws Exception {
        this.setParams();
//        calc.setProperty(TransferEntropyCalculator.K_PROP_NAME, String.valueOf(getK()));
        calc.initialise();
        calc.setObservations(source, dest);
        double result = calc.computeAverageLocalOfObservations();           //从这里开始报错，很多传递熵根本没有计算。
//        EmpiricalMeasurementDistribution measDist = calc.computeSignificance(100);      //计算概率

        return result;
    }

    // 加入统计显著性
//    int numPermutationssToCheck,统计检验的一个参数，参考的默认设置为100
    public String  process(double[] source,double[] dest,int numPermutationssToCheck) throws Exception {
//        String valueAndSign = "";
//        this.setParams();             //增加这一步会报错，k,l等参数会无法进入
//        calc.setProperty(TransferEntropyCalculator.K_PROP_NAME, String.valueOf(getK()));
        calc.initialise();
//        System.out.println("initialise");
        calc.setObservations(source, dest);
//        System.out.println("setObservations");
        double result = calc.computeAverageLocalOfObservations();           //从这里开始报错，很多传递熵根本没有计算。
//        System.out.println("computeAverageLocalOfObservations");
        EmpiricalMeasurementDistribution measDist = calc.computeSignificance(numPermutationssToCheck);      //计算概率
//        System.out.println("computeSignificance");
        System.out.println(""+result+","+measDist.pValue);
//        sleep(5);
//        valueAndSign[0] = result;
//        valueAndSign[1] = measDist.pValue;
//        valueAndSign = ""+result+","+measDist.pValue;
        return ""+result+","+measDist.pValue;
    }



    public double computeDiscrete(int[] source, int[] dest) {
        return 0.0;
    }

    public double computeContinuous(double[] source, double[] dest) {
        double result = 0.0;
        try {
//            System.out.println("source length"+source.length);
//            System.out.println("dest length"+dest.length);

            result = process(source,dest);
//            System.out.println("equal!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("computing error!");
        }
        return result;
    }

    //返回结果包含显著性检验概率，是一个double数组
    public String computeContinuous(double[] source, double[] dest,int numPermutationssToCheck) {
        String result = "";
        try {
            System.out.println("source length"+source.length);
            System.out.println("dest length"+dest.length);

            //@0601貌似没有跑到这一步，，应该是
            result = process(source,dest,numPermutationssToCheck);
//            System.out.println("equal!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("computing error!");
        }
        return result;
    }
}



