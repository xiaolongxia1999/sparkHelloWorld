package TransferEntropy;

 import infodynamics.utils.MatrixUtils;
 import infodynamics.utils.RandomGenerator;
 import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;
/**
 * Created by Administrator on 2018/3/28 0028.
 */
public class MultivariateTransferEntropyOnBinaryData {
    public static void main(String[] args) throws Exception {

        // Requires the following imports before the class definition:
// import infodynamics.utils.MatrixUtils;
// import infodynamics.utils.RandomGenerator;
// import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;

// Generate some random binary data.
        int timeSeriesLength = 100;
        RandomGenerator rg = new RandomGenerator();

//        RandomGenerator.generateRandomInts(int var1,int var2) 表示：生成var1行， 0<随机数<var2
//        RandomGenerator.generateRandomInts(int var1,int var2,int var3) 表示：生成var1行*var2列， 0<随机数<var3
        int[][] sourceArray = rg.generateRandomInts(timeSeriesLength, 2, 2);
        int[][] sourceArray2 = rg.generateRandomInts(timeSeriesLength, 2, 2);
// Destination variable takes a copy of the first bit of the
//  previous source value in bit 0,
//  and an XOR of the two previous bits of the source in bit 1:
        int[][] destArray = new int[timeSeriesLength][2];
        for (int r = 1; r < timeSeriesLength; r++) {
            // This is a bitwise XOR, but is fine for our purposes
            //  with binary data:
            destArray[r][0] = sourceArray[r - 1][0];
            destArray[r][1]	= sourceArray[r - 1][0] ^ sourceArray[r - 1][1];
        }

// Create a TE calculator and run it.
// Need to represent 4-state variables for the joint destination variable
        TransferEntropyCalculatorDiscrete teCalc=
                new TransferEntropyCalculatorDiscrete(4, 1);
        teCalc.initialise();

// We need to construct the joint values of the dest and source before we pass them in:
        teCalc.addObservations(MatrixUtils.computeCombinedValues(sourceArray, 2),
                MatrixUtils.computeCombinedValues(destArray, 2));

        double result = teCalc.computeAverageLocalOfObservations();
        System.out.printf("For source which the 2 bits are determined from, " +
                "result should be close to 2 bits : %.3f\n", result);

// Check random source:
        teCalc.initialise();
        teCalc.addObservations(MatrixUtils.computeCombinedValues(sourceArray2, 2),
                MatrixUtils.computeCombinedValues(destArray, 2));
        double result2 = teCalc.computeAverageLocalOfObservations();
        System.out.printf("For random source, result should be close to 0 bits " +
                "in theory: %.3f\n", result2);

        System.out.printf("The result for random source is inflated towards 0.3 " +
                        " due to finite observation length (%d). One can verify that the " +
                        "answer is consistent with that from a random source by checking: " +
                        "teCalc.computeSignificance(1000); ans.pValue\n",
                teCalc.getNumObservations());
    }
}


//结果如下：第一行计算的是 t(x(i)->x(i+1)),熵应该很大。
//第二行计算的是t(y(i)->x(i+1)）   ，由于y和x都是随机产生的，互相独立，因此熵值较小。
/**
        For source which the 2 bits are determined from, result should be close to 2 bits : 1.917
        For random source, result should be close to 0 bits in theory: 0.206
        The result for random source is inflated towards 0.3  due to finite observation length (99). One can verify that the answer is consistent with that from a random source by checking: teCalc.computeSignificance(1000); ans.pValue
*/

