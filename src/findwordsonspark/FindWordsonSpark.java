/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package findwordsonspark;

import java.io.IOException;  
import java.net.URI;  
import java.net.URISyntaxException;  


import org.apache.hadoop.fs.FileUtil;  
import org.apache.hadoop.fs.Path; 



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
import java.net.URI;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils; 

/**
 *
 * @author wuhong
 */
public class FindWordsonSpark {

    /**
     * @param args the command line arguments
     */
    
    
    private static int wordLen, MinOccur, MinDoc;
    
    private static long partitionSize;
    
    private static double MinDof;
    
    private static String InputFileName;
    
    private static void setWordLen(int val) { wordLen = val;}
    
    private static void setMinOccur(int val) { MinOccur = val; }
    
    private static void setMinDoc(int val) { MinDoc = val; }
    
    private static void setMinDof(double val) { MinDof = val; }
    
    private static void setInputFileName(String str) { InputFileName = str; }
    
    private static void setPartitionSize(long val) { partitionSize = val;}
    
    public static int getWordLen() { return wordLen; }
    
    public static int getMinOccur() { return MinOccur; }
        
    public static int getMinDoc() { return MinDoc; }
    
    public static double getMinDof() {return MinDof; }
    
    public static String getInputFileName() { return InputFileName; }
    
    public static long getPartitionSize() { return partitionSize; }
    
    
    
    
    
    public static void main(String[] args) {
        // TODO code application logic here
        setWordLen(8);
        setMinOccur(12);
        setMinDoc(60);
        setMinDof(1.5);
        setPartitionSize(4444444);
        setInputFileName(args[0]);
        
        //以上是设置各个参数,抽词的最大长度, 最小频率,最小凝固度, 最小自由度, 单个PARTITION大小, 输入文件名
        
        
        long FileLength = 0;
        
        try {
            FileLength = getFileLength(getInputFileName());
        } catch (IOException e) {
            System.out.println("No such file");
        }
        
        //获取文本大小
        
        
        SparkConf SPconf = new SparkConf().setAppName("FindWordsonSpark");
        
        //设置模式 加上.setMaster("local")为单机模式.
        
        JavaSparkContext sc = new JavaSparkContext(SPconf);
        
        int num = (int)(FileLength / getPartitionSize()) + 1;
        
        //根据文本大小,计算分块数
        
        JavaRDD<String> adFile = sc.textFile(getInputFileName(), num);
        
        JavaRDD<String> ad = adFile.map(new WashContext());
        
        //原来文本按照一条条ad建立 RDD<String> 然后map清洗每一条ad
        
        JavaRDD<Tuple2<String, Integer>> wf = ad.mapPartitions(new PartitionChange()); 
        
        //每个partition分别抽词,得到每个Partition的 Word-Freqency
        
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
        
        //reduce~
        
        wfPair.unpersist();
        
        PairFunction<Tuple2<String, Integer>, Integer, String> funcwf2fw = new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tmp) {
                        return new Tuple2<Integer, String>(tmp._2(), tmp._1());
                    }
                };
        
        
        JavaPairRDD<Integer, String> resultRDD =  TotalWfPair.mapToPair(funcwf2fw).sortByKey(false);
        
        //按照频率sort
       
        resultRDD.saveAsTextFile(args[1]);
        
        //保存结果
    }
    
    private static long getFileLength(String file) throws IOException {
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        long rs = fs.getLength(path);
        return rs;
        
    }
    
 }