package TransferEntropy.infodynamics.measures.discrete1;

/*
created at @ 2018/04/12
 */

import infodynamics.utils.ArrayFileReader;
import infodynamics.utils.MatrixUtils;

import infodynamics.measures.discrete.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class fromAutoAnalyserDerivedCode {

    //存放每个（k,l）组合下的传递熵值及位置
    public HashMap<String,Double> map = new HashMap<String,Double>();


    public fromAutoAnalyserDerivedCode(){
    }
//    public fromAutoAnalyserDerivedCode(HashMap map1){
//        this.map = map1;
//    }


    public static void  printArray2dDouble(double[][] array2d){
        for (int i=0;i<array2d.length;i++){
            System.out.print("row"+(i+1)+": ");
            for (int j=0;j<array2d[0].length;j++){
                if(j!=array2d[0].length-1){
                    System.out.print(array2d[i][j]+" ");
                }else {
                    System.out.print("\n");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {



        // 0. Load/prepare the data:
        String dataFile = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2_withoutAllNull\\part-00000";
//        String dataFile = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2_withoutAllNull\\part-00001.csv";
        ArrayFileReader afr = new ArrayFileReader(dataFile);
        int[][] data = afr.getInt2DMatrix();
        int dataColNum = data[0].length;
        System.out.println("dataColNum:"+dataColNum);
        // 1. Construct the calculator:
        int base = 3;
        int destHistoryEmbedLength = 1;         //即k的最长时滞;   x的时滞  。 下面计算<k的各个k值的长度
        int destEmbeddingDelay =1;
        int sourceHistoryEmbeddingLength=1;     //即l:   y的时滞。   l也是同理
        int sourceEmbeddingDelay=1;
        int delay=2;

//        TransferEntropyCalculatorDiscrete calc = new TransferEntropyCalculatorDiscrete(base, destHistoryEmbedLength, destEmbeddingDelay, sourceHistoryEmbeddingLength, sourceEmbeddingDelay, delay);
        // 2. No other properties to set for discrete calculators.

        fromAutoAnalyserDerivedCode fAADC = new fromAutoAnalyserDerivedCode();
//        int rowNum = data.length;
//        int colNum = data[0].length;
//        HashMap hashMap = new HashMap();

//        double 4darray[][] = new double[destHistoryEmbedLength][sourceHistoryEmbeddingLength];

        double fourDarray[][][][] = new double[destHistoryEmbedLength][sourceHistoryEmbeddingLength][dataColNum][dataColNum];


        //对不同的（k,l)，将每个传递熵值的矩阵，放到k*l的二维数组中（数组的每一个元素是一个矩阵）
        for(int k=1;k<destHistoryEmbedLength+1;k++){
            for(int l=1;l<sourceHistoryEmbeddingLength+1;l++){
                TransferEntropyCalculatorDiscrete calc = new TransferEntropyCalculatorDiscrete(base,k, destEmbeddingDelay,l, sourceEmbeddingDelay, delay);
                double[][] TE_matrix = computeTEMatrix(calc, data);
                //                System.out.println((TE_matrix));

//                fAADC.getMaxFrom2dArray(TE_matrix);
//                System.out.println(fAADC.map.size());
                for(int m=0;m<dataColNum;m++){
                    for(int n=0;n<dataColNum;n++){
                        fourDarray[k-1][l-1][m][n] = TE_matrix[m][n];           //k和l都是从 1开始的，而数组是从0开始的。
                    }
                }
//                printArray2dDouble(TE_matrix);
            }
        }

        double[][] doubles = new double[fourDarray.length][fourDarray[0].length];
        double maxValue =0.0;
        for(int dest=0;dest<fourDarray[0][0].length;dest++){
            for(int source=0;source<fourDarray[0][0][0].length;source++){
                for(int k=0;k<doubles.length;k++){
                    for(int l=0;l<doubles[0].length;l++){
                        doubles[k][l] = fourDarray[k][l][dest][source];
                    }
                }
                fAADC.getMaxFrom2dArray(doubles);
            }
        }
//
//        for(int i=0;i<fourDarray.length;i++){
//            for(int j=0;j<fourDarray[0].length;j++){
//                System.out.println("k="+i+",l="+j+","+fourDarray[i][j][0][1]);
//            }
//        }


        Iterator it = fAADC.map.entrySet().iterator();


        while (it.hasNext()){
            Map.Entry next = (Map.Entry) it.next();
            System.out.println("key:"+next.getKey()+","+"value:"+next.getValue());
//            System.out.println(next.getValue());

        }

//        Set set = fAADC.map.keySet();
//        System.out.println(set);
//        Iterator it = set.iterator();
//
//
//        //打印
//        while(it.hasNext()){
////            System.out.println(it.next());
//            String j = (String)it.next();
//            System.out.println(j.toString());
//            System.out.println("location:"+(String)j+",maxValue:"+fAADC.map.get(j)) ;
//        }




        //cl:我添加的代码
//        double TE_matrix[][] =new double[colNum][colNum];
//
////        System.out.println(rowNum);
////        System.out.println(colNum);
//        // Compute for all pairs:
//        for (int s = 0; s < colNum; s++) {
//            for (int d = 0; d < colNum; d++) {
//                // For each source-dest pair:
//                if (s == d) {
//                    continue;
//                }
//                int[] source = MatrixUtils.selectColumn(data, s);
//                int[] destination = MatrixUtils.selectColumn(data, d);
//
//                // 3. Initialise the calculator for (re-)use:
//                calc.initialise();
//                // 4. Supply the sample data:
//                calc.addObservations(source, destination);
//                // 5. Compute the estimate:
//                double result = calc.computeAverageLocalOfObservations();
//
//                TE_matrix[s][d] = result;
////                System.out.printf("TE_Discrete(col_%d -> col_%d) = %.4f bits\n",s, d, result);
//            }
//        }
            //定义上面成函数，待完成
//        printArray2dDouble(TE_matrix);
    }

    //计算data1的各列之间的传递熵
    public static double[][] computeTEMatrix(TransferEntropyCalculatorDiscrete calc,int[][] data1){
        int colNum = data1[0].length;
        System.out.println("colNum:"+colNum);
        double te_matrix[][] = new double[colNum][colNum];

        for (int s = 0; s < colNum; s++) {
            for (int d = 0; d < colNum; d++) {
                // For each source-dest pair:
                if (s == d) {
                    continue;
                }
                int[] source = MatrixUtils.selectColumn(data1, s);
                int[] destination = MatrixUtils.selectColumn(data1, d);

                // 3. Initialise the calculator for (re-)use:
                calc.initialise();
                // 4. Supply the sample data:
                calc.addObservations(source, destination);
                // 5. Compute the estimate:
                double result = calc.computeAverageLocalOfObservations();
                te_matrix[s][d] = result;
            }
        }
        return te_matrix;

    }

//    注意：k是x的滞后阶数，l是y的滞后阶数。。——————k对应的是dest,l对应的是source
//    这里修改下！
    //传递熵越小，x越独立于y，因此y越不是x的原因。所以此处应该是求传递熵值最小的那个，作为传递熵值吧！
    //简单的交换， 排序也是这个原理
    public  void getMaxFrom2dArray(double[][] array){
        int rowIndex = 0;
        int colIndex = 0;
        double maxValue = array[0][0];
        for(int i=0;i<array.length;i++){
            for(int j=0;j<array[0].length;j++){
                if(array[i][j]>maxValue){
                    maxValue = array[i][j];
                    rowIndex = i;
                    colIndex = j;
                }
            }
        }
        String str = "row:"+rowIndex+","+"column:"+colIndex;
        map.put(str,maxValue);
        return;
    }
}


