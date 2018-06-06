package TransferEntropy.JavaUtils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2018/5/15 0015.
 */
//出处：https://blog.csdn.net/u010466329/article/details/77869405
//    很多换行符都是用的\n，windows下有效，linux下不知是否有效
public class CsvToJson {

    public static String getJSONFromFile(String fileName,String separator) throws IOException {
        byte[] bytes =null;
        bytes = FileUtils.readFileToByteArray(new File(fileName));
        String csv = new String(bytes, "GBK");
        System.out.println(csv);

        return getJSON(csv,separator).replace("\r","").replace("\n","").replace("\b","").replaceAll("\\s*","");

    }
    public static String getJSON(String content,String separator){
        StringBuilder sb = new StringBuilder("[\n");
        String csv = content;
        String[] csvValues = csv.split("\n");//原网址是"\n"
        String[] header = csvValues[0].split(separator);

        for(int i=1;i<csvValues.length;i++){
            sb.append("\t").append("{").append("\n");
            String[] tmp = csvValues[i].split(separator);

            for (int j=0;j<tmp.length;j++){
                sb.append("\t").append("\t\"").append(header[j].replaceAll("\\s*", "").replace("\"", "")).append("\":\"")
                        .append(tmp[j].replaceAll("\\s*", "").replace("\"", "")).append("\"");
                if(j<tmp.length-1){
                    sb.append(",\n");
                }else {
                    sb.append("\n");
                }
            }
            if (i<csvValues.length-1){
                sb.append("\t},\n");
            }else {
                sb.append("\t}\n");
            }

        }
        sb.append("]");
        return  sb.toString();

    }

    public static void main(String[] args) throws IOException {
        String path = "C:\\Users\\Administrator\\Desktop\\excel\\1.csv";
        System.out.println(getJSONFromFile(path,"\\,"));
    }
}
