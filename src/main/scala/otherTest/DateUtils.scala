package otherTest

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
  * Created by Administrator on 2018/4/9 0009.
  */

//format_String形如——"yyyy-MM-dd H:mm:ss"

//注意，此处用的是北京时间，不是UTC时间
class MyDateUtils {
  def formatData(line:Date,format_String:String)={
//    val UTCtime = line
    val date=new SimpleDateFormat(format_String,Locale.UK)
    val dateFormated=date.format(line)
    val dateFf=date.parse(dateFormated).getTime
    dateFf
  }
}

object MyDateUtils{
  def main(args: Array[String]): Unit = {
    val line = new Date("1970/1/1 0:0:0")
    val format_String = "yyyy/M/D H:mm:ss"
    println(new MyDateUtils().formatData(line,format_String));
  }
}
