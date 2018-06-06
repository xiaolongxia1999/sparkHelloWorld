package TransferEntropy;
 import infodynamics.utils.RandomGenerator;
 import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;
/**
 * Created by Administrator on 2018/3/28 0028.
 */
public class TransferEntropyOnMultidimensionalBinaryData {
    public static void main(String[] args) {
// Requires the following imports before the class definition:
// import infodynamics.utils.RandomGenerator;
// import infodynamics.measures.discrete.TransferEntropyCalculatorDiscrete;



        int timeSteps = 2;
        int variables = 100;
        RandomGenerator rg = new RandomGenerator();
        rg.setSeed(System.currentTimeMillis());

// Create many columns in a multidimensional array (2 rows by 100 columns),
//  where the next time step (row 2) copies the value of the column on the left
//  from the previous time step (row 1):
        int[][] twoDTimeSeries = new int[timeSteps][];
        twoDTimeSeries[0] = rg.generateRandomInts(variables, 2);    //表示产生服从0-2均匀分布的100个随机数
        twoDTimeSeries[1] = new int[variables];
        twoDTimeSeries[1][0] = twoDTimeSeries[0][variables - 1];
        System.arraycopy(twoDTimeSeries[0], 0, twoDTimeSeries[1], 1, variables - 1);

// Create a TE calculator and run it:
        TransferEntropyCalculatorDiscrete teCalc=
                new TransferEntropyCalculatorDiscrete(2, 1);
        teCalc.initialise();
// Add observations of transfer across one cell to the right (j=1)
//  per time step:
        teCalc.addObservations(twoDTimeSeries, 1);

        double result2D = teCalc.computeAverageLocalOfObservations();
        System.out.printf("The result should be close to 1 bit here, " +
                "since we are executing copy operations of what is effectively " +
                "a random bit to each cell here: %.3f bits\n", result2D);

    }
}
