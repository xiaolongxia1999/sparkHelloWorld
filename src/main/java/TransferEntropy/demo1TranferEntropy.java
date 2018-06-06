package TransferEntropy;

 import infodynamics.utils.RandomGenerator;
 import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;

/**
 * Created by Administrator on 2018/3/28 0028.
 */
public class demo1TranferEntropy {

    //输入一个0-1变量的二元int数组，获取其0、1分别出现的概率
    public static Double[] countBinaryPossibilities(int[] array) {
        Double array_p0_p1[] = new Double[2];
        Double count_zero =0.0D;
        Double count_one =0.0D;
        for(int i=0;i<array.length;i++){
            if(array[i]==0){
                ++count_zero;
            }else{
                ++count_one;
            }
        }
        Double p_zero = 0.0D+count_zero/array.length;
        Double p_one = 0.0D+count_one/array.length;
        array_p0_p1[0]=p_zero;
        array_p0_p1[1]=p_one;
        System.out.println("array length:"+array.length);
        System.out.println("0's pValue:"+p_zero+"    "+"1's pValue:"+p_one);
        return  array_p0_p1;
    }

    public  static Double Entropy(Double[] array_p){
        Double entropy = 0.0D;

        for(int i=0;i<array_p.length;i++){
             entropy+= -array_p[i]*Math.log(array_p[i])/Math.log(2);    //使用换底公式，java中是e为底，换成2为底，见：https://blog.csdn.net/f1024042400/article/details/52064652
        }
        return entropy;
    }

    //        封装了JIDT对二元变量的序列y对x的传递熵计算结果，分别传入int[]x和int[] y，得到Te(y->x)
    public  double tranferEntroyOfTwoSeriesDicrete(int[] x,int[] y) {
        TransferEntropyCalculatorDiscrete tecd = new TransferEntropyCalculatorDiscrete(2, 1);
        tecd.initialise();
        tecd.addObservations(x, y);
        double result_TransferEntropy = tecd.computeAverageLocalOfObservations();
        return result_TransferEntropy;
    }

    public static void main(String[] args) {
        //二元值的y对x的传递熵计算。 其中，teCalc.addObservations(sourceArray, destArray)的2个参数分别代表x和y
        //表示后者对前者的传递熵数值。
        TestTransferEntropyBinaryDiscrete testTransferEntropyBinaryDiscrete = new TestTransferEntropyBinaryDiscrete().invoke();


        int[] sourceArray = testTransferEntropyBinaryDiscrete.getSourceArray();
        int[] sourceArray2 = testTransferEntropyBinaryDiscrete.getSourceArray2();
        int destArray[] = new int[sourceArray.length] ;
        System.arraycopy(sourceArray, 0, destArray, 1, destArray.length - 1);


        //测试数组生成概率
        Double p1[] = countBinaryPossibilities(sourceArray);
        Double p2[] = countBinaryPossibilities(sourceArray2);
        //测试数组的熵值
        System.out.println("sourceArray's entropy is:"+Entropy(p1));
        System.out.println("sourceArray2's entropy is:"+Entropy(p2));
        System.out.println(Entropy(new Double[]{0.01,0.01,0.98}));

        demo1TranferEntropy demo1 = new demo1TranferEntropy();
        System.out.println(demo1.tranferEntroyOfTwoSeriesDicrete(destArray,sourceArray2));

    }

    private static class TestTransferEntropyBinaryDiscrete {
        private int[] sourceArray;
        private int[] sourceArray2;

        public int[] getSourceArray() {
            return sourceArray;
        }

        public int[] getSourceArray2() {
            return sourceArray2;
        }

        public TestTransferEntropyBinaryDiscrete invoke() {
            // Requires the following imports before the class definition:
// import infodynamics.utils.RandomGenerator;
// import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;

            int arrayLengths = 100;
            RandomGenerator rg = new RandomGenerator();

// Generate some random binary data:
            sourceArray = rg.generateRandomInts(arrayLengths, 2);
            int[] destArray = new int[arrayLengths];
            destArray[0] = 0;
            System.arraycopy(sourceArray, 0, destArray, 1, arrayLengths - 1);
            sourceArray2 = rg.generateRandomInts(arrayLengths, 2);

            //打印两个原始Array,取值是二元的，0-1,即只有0-1两种状态
            for(int i=0;i<sourceArray2.length;i++){
                System.out.println("the"+i+"element is_"+"sourceArray:"+sourceArray[i]+" sourceArray2:"+sourceArray2[i]+"\n");
            }

// Create a TE calculator and run it:
            TransferEntropyCalculatorDiscrete teCalc=
                    new TransferEntropyCalculatorDiscrete(2, 1);
            teCalc.initialise();
            teCalc.addObservations(sourceArray, destArray);
            double result = teCalc.computeAverageLocalOfObservations();
            System.out.printf("For copied source, result should be close to 1 bit : %.3f bits\n", result);
            teCalc.initialise();
            teCalc.addObservations(sourceArray2, destArray);
            double result2 = teCalc.computeAverageLocalOfObservations();
            System.out.printf("For random source, result should be close to 0 bits: %.3f bits\n", result2);
            return this;
        }




    }
}

/**
 * 运行结果如下：
 * For copied source, result should be close to 1 bit : 0.986 bits
 * For random source, result should be close to 0 bits: 0.027 bits
 *
 * 这个结果是符合预期的。
 * 1.destArray完全是copy了sourceArray的，二者完全互为因果。传递熵较大，接近1，说明sourceArray完全是destArray的因
 * 2.sourceArray2是随机生成的序列，二者其实非常独立，传递熵较小，说明sourceArray2的发生，对destArray的不确定性，没有什么影响。
 *
 * 注意：Te(y->x)越大，说明x对y的依赖越大，或者说y是x的“原因”的程度更大。
 */
