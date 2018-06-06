package TransferEntropy.spark;

import infodynamics.utils.ArrayFileReader;
import infodynamics.utils.MatrixUtils;

import infodynamics.measures.discrete.*;
/**
 * Created by Administrator on 2018/4/26 0026.
 */
public class computeTwoSeriesTE {

    private int base;
    private int k;
    private int k_tau;
    private int l;
    private int l_tau;
    private int delay;

    public computeTwoSeriesTE(int base, int k, int k_tau, int l, int l_tau, int delay) {
        this.base = base;
        this.k = k;
        this.k_tau = k_tau;
        this.l = l;
        this.l_tau = l_tau;
        this.delay = delay;
    }

    public computeTwoSeriesTE() {
        super();
    }

    public int getBase() {
        return base;
    }

    public void setBase(int base) {
        this.base = base;
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

    public double compute(int[] source,int[] dest){
        TransferEntropyCalculatorDiscrete cal = new TransferEntropyCalculatorDiscrete(base,k,k_tau,l,l_tau,delay);
        cal.initialise();
        cal.addObservations(source,dest);
        double result = cal.computeAverageLocalOfObservations();
        return  result;
    }

    public static void main(String[] args) throws Exception {

        // 0. Load/prepare the data:
        //String dataFile = "E:\\InstallPacksges\\JIDT\\infodynamics-dist-1.4\\demos\\data\\2coupledBinaryColsUseK2.txt";

        //1w * 14 的离散数据
        //String dataFile = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2_withoutAllNull\\part-00000.csv";
        // 28w * 14的离散数据
        String dataFile = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2\\withOutTimeAndFields.csv";

        ArrayFileReader afr = new ArrayFileReader(dataFile);
        int[][] data = afr.getInt2DMatrix();
        int[] source = MatrixUtils.selectColumn(data, 10);
        int[] destination = MatrixUtils.selectColumn(data, 0);

        // 1. Construct the calculator:
        TransferEntropyCalculatorDiscrete calc1
                = new TransferEntropyCalculatorDiscrete(3, 5, 1, 1, 1, 5);
        // 2. No other properties to set for discrete calculators.
        // 3. Initialise the calculator for (re-)use:
        calc1.initialise();
        // 4. Supply the sample data:
        calc1.addObservations(source, destination);
        // 5. Compute the estimate:
        double result = calc1.computeAverageLocalOfObservations();

        System.out.printf("TE_Discrete(col_0 -> col_1) = %.9f bits\n",
                result);

//        computeTwoSeriesTE cal1 = new computeTwoSeriesTE(2, 1, 1, 1, 1, 1);
//        double result1 = cal1.compute(source, destination);
//        System.out.println(result);
//
//        System.out.println(result1==result);
    }


}