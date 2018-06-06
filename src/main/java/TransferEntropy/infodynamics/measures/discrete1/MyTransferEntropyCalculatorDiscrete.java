package TransferEntropy.infodynamics.measures.discrete1;


import com.csvreader.CsvReader;
import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import TransferEntropy.demo1TranferEntropy;

/**
 * Created by Administrator on 2018/4/11 0011.
 */
//封装了二元时间序列的传递熵计算，结果返回传递熵值
public class MyTransferEntropyCalculatorDiscrete {

    /**
     * Create a new TE calculator for the given base, destination and source history embedding lengths
     *  and delays.
     *
     * @param base number of quantisation levels for each variable.
     *        E.g. binary variables are in base-2.
     * @param destHistoryEmbedLength embedded history length of the destination to condition on -
     *        this is k in Schreiber's notation.
     * @param destEmbeddingDelay embedding delay of the destination for conditioning on -
     *        this is the delay between each of the k samples from the past history
     * @param sourceHistoryEmbeddingLength embedded history length of the source to include -
     *        this is l in Schreiber's notation.
     * @param sourceEmbeddingDelay embedding delay of the source -
     *        this is the delay between each of the l samples from the past history
     * @param delay source-destination delay to consider the information transfer across
     * 		  (should be >= 0, default is 1)
     */
    public static double tranferEntroyOfTwoSeriesDicrete1(int[] x,int[] y,int base, int destHistoryEmbedLength, int destEmbeddingDelay,
                                                          int sourceHistoryEmbeddingLength, int sourceEmbeddingDelay, int delay) {

//        TransferEntropyCalculatorDiscrete tecd = new TransferEntropyCalculatorDiscrete(base, destHistoryEmbedLength,destEmbeddingDelay,
//        sourceHistoryEmbeddingLength, sourceEmbeddingDelay,delay);  //这里设置base=3，有-1,0,1这3种状态

        TransferEntropyCalculatorDiscrete tecd = new TransferEntropyCalculatorDiscrete(base, destHistoryEmbedLength,1,
                sourceHistoryEmbeddingLength, 1,1);
        //这里其他都设置为1，  只有Base(离散状态的个数)和K：destHistoryEmbedLength 和L： sourceHistoryEmbedLength————是可变的。

        tecd.initialise();
        tecd.addObservations(x, y);
        double result_TransferEntropy = tecd.computeAverageLocalOfObservations();
        return result_TransferEntropy;
    }

    //打印浮点型二维数组——————考虑用反射来设置打印的二维数组类型。
    public static void  printArray2dDouble(Double[][] array2d){
        for (int i=0;i<array2d.length;i++){
            for (int j=0;j<array2d[0].length;j++){
                if(j!=array2d[0].length-1){
                    System.out.print(array2d[i][j]+" ");
                }else {
                    System.out.print("\n");
                }
            }
        }
    }

    public static void  printArray2dDouble(double[][] array2d){
        for (int i=0;i<array2d.length;i++){
            for (int j=0;j<array2d[0].length;j++){
                if(j!=array2d[0].length-1){
                    System.out.print(array2d[i][j]+" ");
                }else {
                    System.out.print("\n");
                }
            }
        }
    }

//利用反射打印二维数组
//    public static void printArray2dByName(T[][] data,String typeName,int rowNum,int colNum){
//        Class.forName(typeName).getClasses()
//        for (int i=0;i<array2d.length;i++){
//            for (int j=0;j<array2d[0].length;j++){
//                if(j!=array2d[0].length-1){
//                    System.out.print(array2d[i][j]+" ");
//                }else {
//                    System.out.print("\n");
//                }
//            }
//        }
//    }

    public static void  printArray2dInt(int[][] array2d){
        for (int i=0;i<array2d.length;i++){
            for (int j=0;j<array2d[0].length;j++){
                if(j!=array2d[0].length-1){
                    System.out.print(array2d[i][j]+" ");
                }else {
                    System.out.print("\n");
                }
            }
        }
    }

    //读取csv，不要头部，读取的数据必须全是int类型，这里是返回其int[][]
    public static int[][] readCsv(String filePath) throws FileNotFoundException {
        try {
            // 创建CSV读对象
            CsvReader csvReader = new CsvReader(filePath);
            // 读表头
//            csvReader.readHeaders();
            ArrayList<String> list = new ArrayList<String>();

//            System.out.println(rowCount);
            while (csvReader.readRecord()){
                // 读一整行
                list.add(csvReader.getRawRecord());
            }
            int columnCount = list.get(0).split(",").length;
            int[][] intArray = new int[list.size()][columnCount];
            for(int i=0;i<intArray.length;i++){
                for(int j=0;j<intArray[0].length;j++){
                    intArray[i][j] = Integer.valueOf(list.get(i).split(",")[j]).intValue();
                }
            }
//            System.out.println(intArray.length);
//            System.out.println(intArray[0].length);
//            printArray2dInt(intArray);
            System.out.println("done!");
            return intArray;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    //测试报警离散状态（-1,0,1）的表：E:\bonc\工业第二期需求\data_sample\单元测试数据\datas\DiscreteResult\DiscreteStateRusultAlarm.csv
    public static void main(String[] args) throws FileNotFoundException {

//        String filePath = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult\\DiscreteStateRusultAlarm.csv";
        String filePath = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2_withoutAllNull\\part-00000";

        int sourceData[][] = readCsv(filePath);

//        printArray2dInt(sourceData);

//        int m = 100;
//        int n = 14;
//        int sourceData[][] = new int[m][n];


        //对于指定k和l，计算y和x的两两之间的传递熵

//        Double[][] TEmatrix = new Double[sourceData[0].length][sourceData[0].length];   //存放TE(sourceData第i列——>sourceData第j列)的传递熵矩阵
//
//        int k =10;
//        int l =1;
//        for (int i=0;i<TEmatrix.length;i++){
//            for (int j=0;j<TEmatrix.length;j++){
//                TEmatrix[i][j] =  tranferEntroyOfTwoSeriesDicrete1(sourceData[i],sourceData[j],3,k,1,l,1,1);
//                System.out.println("sensor i,j,value is:"+i+","+j+","+TEmatrix[i][j]);
//            }
//        }


        int src[] = new int[]{0,1,0,1,1,0,1,0,1,0};         //刚开始数组内元素全为1，因此传递熵总为0
        int dest[] = new int[]{0,0,1,1,1,1,1,0,1,1};
//        TransferEntropyCalculatorDiscrete tecd = new TransferEntropyCalculatorDiscrete(2, 1);  //这里设置base=3，有-1,0,1这3种状态
//        tecd.initialise();
//        tecd.addObservations(src, dest);
//        double result_TransferEntropy = tecd.computeAverageLocalOfObservations();
        //这个是没问题的
//        demo1TranferEntropy demo1 = new demo1TranferEntropy();
//        double result = demo1.tranferEntroyOfTwoSeriesDicrete(src,dest);
//        double result = tranferEntroyOfTwoSeriesDicrete1(src,dest);

//        RandomGenerator rg = new RandomGenerator();
//        int src[] =  rg.generateRandomInts(10,2);
//        int dest[] = rg.generateRandomInts(10,2);
//        System.out.println(result);

        int base = 3;
        int kmax = 5;
        int lmax = 4;
        double[][] testMatrix = new double[kmax][lmax];

        for(int i=0;i<kmax;i++){
            for(int j=0;j<lmax;j++){

                testMatrix[i][j]= tranferEntroyOfTwoSeriesDicrete1(src,dest,base,i+1,1,j+1,1,1);
                //计算出的实际矩阵是 kmax*(lmax-1)大小的， 当lmax=1时，不打印值。
//                System.out.println("i");
//                System.out.println(testMatrix[i][j]);
            }
        }
        printArray2dDouble(testMatrix);
//        System.out.println(tranferEntroyOfTwoSeriesDicrete1(sourceData[0],sourceData[1]));
//        printArray2dDouble(TEmatrix);

    }

}
