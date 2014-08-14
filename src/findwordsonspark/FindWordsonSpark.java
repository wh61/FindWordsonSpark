/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package findwordsonspark;


import java.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import TransformFunctions.*;
import findwordsonspark.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

/**
 *
 * @author wuhong
 */
public class FindWordsonSpark {

    /**
     * @param args the command line arguments
     */
    
    
    private static int wordLen, MinOccur, MinDoc;
    
    private static double MinDof;
    
    private static String InputFileName;
    
    private static void setWordLen(int val) { wordLen = val;}
    
    private static void setMinOccur(int val) { MinOccur = val; }
    
    private static void setMinDoc(int val) { MinDoc = val; }
    
    private static void setMinDof(double val) { MinDof = val; }
    
    private static void setInputFileName(String str) { InputFileName = str; }
    
    public static int getWordLen() { return wordLen; }
    
    public static int getMinOccur() { return MinOccur; }
        
    public static int getMinDoc() { return MinDoc; }
    
    public static double getMinDof() {return MinDof; }
    
    public static String getInputFileName() { return InputFileName; }
    
    
    
    public static void main(String[] args) {
        // TODO code application logic here
        setWordLen(5);
        setMinOccur(30);
        setMinDoc(40);
        setMinDof(2.1);
        setInputFileName(args[0]);
        
        SparkConf conf = new SparkConf().setAppName("FindWordsonSpark");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> adFile = sc.textFile(getInputFileName(), 15);
        
        JavaRDD<String> ad = adFile.map(new WashContext());
        
        JavaRDD<Tuple2<String, Integer>> wf = ad.mapPartitions(new PartitionChange());                     
        
        JavaPairRDD<String, Integer> wfPair = wf.mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tmp) {
                        return new Tuple2(tmp._1(), tmp._2());
                    }
                }
         );
        
        wf.unpersist();
        
        Function2<Integer, Integer, Integer> funcSum = new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                };
        
        JavaPairRDD<String, Integer> TotalWfPair = wfPair.reduceByKey(funcSum);
        
        
        wfPair.unpersist();
        
        PairFunction<Tuple2<String, Integer>, Integer, String> funcwf2fw = new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tmp) {
                        return new Tuple2<Integer, String>(tmp._2(), tmp._1());
                    }
                };
        
        
        JavaPairRDD<Integer, String> resultRDD =  TotalWfPair.mapToPair(funcwf2fw).sortByKey(false);
        
       
        resultRDD.saveAsTextFile(args[1]);
       
        
    }
}