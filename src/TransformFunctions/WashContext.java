/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package TransformFunctions;

import java.*;
import java.util.*;
import findwordsonspark.FindWordsonSpark;
import org.apache.spark.api.java.function.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
/**
 *
 * @author wuhong
 */
public class WashContext implements Function<String, String>{
    public String call(String sourceText) {
        
       int sz = sourceText.length();
    
       StringBuilder str = new StringBuilder(sz);
       int lastchar = 0;
       for (char ch : sourceText.toCharArray()) {
           if (('0' <= ch && ch <= '9') || ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z')) {
               lastchar++;
               if (lastchar < 8) 
                   str.append(ch);
           } else if (0x4E00 <= ch && ch <= 0x9FA5) {
               str.append(ch);
               lastchar = 0;
           } else {
           }
       }
       return new String(str);
    }
}