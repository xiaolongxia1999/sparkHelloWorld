package TransferEntropy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Administrator on 2018/4/16 0016.
 */
public class test {
    public static void main(String[] args) {
        HashMap map = new HashMap();
        map.put("1",2.0);
        map.put("1",2.0);
        map.put("2",3.0);

        Iterator it = map.entrySet().iterator();
        ArrayList list = new ArrayList();
        list.add(1.1);
        list.add(2.1d);


        while (it.hasNext()){
            Map.Entry next = (Map.Entry) it.next();
            System.out.println(next.getKey());
            System.out.println(next.getValue());

        }
    }
}
