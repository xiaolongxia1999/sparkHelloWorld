package TransferEntropy.JavaUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/5/11 0011.
 */



public class JavaUtils {

    //将scala中Array[Double]转成double[]
    public static double[] convertToJavaArray(List<String> list){
        int size = list.size();
//        ArrayList list1 = (ArrayList)list;             //list1里的类型全部擦除了，但强制转换时应该仍然是String
//        Double arr[] = (Double[])list1.toArray();
        double doubleArray[] = new double[size];
        for(int i=0;i<size;i++){
            doubleArray[i]= Double.parseDouble((String) list.get(i)); //(Double)list.get(i) ;//Double.parseDouble((String) list1.get(i)) ;
        }
        return doubleArray;
    }


    public static void main(String[] args) {
        ArrayList list = new ArrayList();
        list.add(1.0);
        list.add(2.0);

        List l1 = (List)list;

        double[] doubles = convertToJavaArray(l1);

        double a = 1.0;
        System.out.println(a==doubles[0]);
        for(int i=0;i<doubles.length;i++){
            System.out.println(doubles[i]);
        }
    }
}
