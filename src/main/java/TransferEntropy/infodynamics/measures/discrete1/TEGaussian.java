package TransferEntropy.infodynamics.measures.discrete1;


import infodynamics.utils.ArrayFileReader;
import infodynamics.utils.MatrixUtils;

import infodynamics.measures.continuous.*;
import infodynamics.measures.continuous.gaussian.*;
/**
 * Created by Administrator on 2018/4/18 0018.
 */
public class TEGaussian {
    public static void main(String[] args) throws Exception {

        // 0. Load/prepare the data:
        String dataFile = "E:\\InstallPacksges\\JIDT\\infodynamics-dist-1.4\\demos\\data\\10ColsRandomGaussian-1.txt";
        ArrayFileReader afr = new ArrayFileReader(dataFile);
        double[][] data = afr.getDouble2DMatrix();
        // 1. Construct the calculator:
        TransferEntropyCalculatorGaussian calc;
        calc = new TransferEntropyCalculatorGaussian();
        // 2. Set any properties to non-default values:
        // No properties were set to non-default values

        // Compute for all pairs:
        for (int s = 0; s < 10; s++) {
            for (int d = 0; d < 10; d++) {
                // For each source-dest pair:
                if (s == d) {
                    continue;
                }
                double[] source = MatrixUtils.selectColumn(data, s);
                double[] destination = MatrixUtils.selectColumn(data, d);

                // 3. Initialise the calculator for (re-)use:
                calc.initialise();
                // 4. Supply the sample data:
                calc.setObservations(source, destination);
                // 5. Compute the estimate:
                double result = calc.computeAverageLocalOfObservations();

                System.out.printf("TE_Gaussian(col_%d -> col_%d) = %.4f nats\n",
                        s, d, result);
            }
        }

    }
}
