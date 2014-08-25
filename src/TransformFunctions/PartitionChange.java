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



/**
 * 对一个Partition的文本进行抽词.
 * 输入Parition文本, 输出Word-Freqency(key-value)对
 */




public class PartitionChange implements FlatMapFunction<Iterator<String>, Tuple2<String, Integer>> {
    
    private int MinOccur = FindWordsonSpark.getMinOccur();
    
    private int MinDoc = FindWordsonSpark.getMinDoc();
    
    private double MinDof = FindWordsonSpark.getMinDof();
    
    private int wordLen = FindWordsonSpark.getWordLen();
    
    private int totalLen;
    
    private void setTotalLen(int val) { totalLen = val; }
    
    public Map<String, Integer> MainFunction(List<String> adList) {
        
        // 把Partition里的Iteratable String首尾相接起来变成一个大的String
        StringBuilder buildstr = new StringBuilder();
        for (String s : adList) {
            buildstr.append(s);
        }
        
        setTotalLen(buildstr.length());
               
        //String后插入若干个'a', 防止最后几个词长度不够,造成越界
        for (int i = 0; i <= wordLen; ++i) {
            buildstr.append('a');
        }     
        
        String str = new String(buildstr);
        
        //大串取反, 因为要取反向前缀计算左自由度
        buildstr.reverse();
        
        String strReverse = new String(buildstr);
        
        buildstr = null;
        
        //建立正向前缀数组
        List<String> postfixList = buildPostfixList(str);
        
        str = null;
        
        //计算候选词-词频,同时过滤去频率很小的
        Map<String, Integer> FreqencyMap = CountAllFreqency(postfixList);
        
        //对上一步通过词频筛选的词进一步计算凝固度,并且筛选出凝固度达到要求的候选词.
        Set<String> PreValidWords = calculateDoc(FreqencyMap).keySet();
        
        //对筛选出来凝固度与频率的达到要求的词计算其右自由熵
        Map<String, Double> RDOF = DOF(PreValidWords, FreqencyMap, postfixList, false);
        
        //建立反方向前缀数组
        postfixList = buildPostfixList(strReverse);
        
        strReverse = null;
        
        //计算右自由熵
        Map<String, Double> LDOF = DOF(PreValidWords, FreqencyMap, postfixList, true);
        
        postfixList = null;
        
        
        Map<String, Integer> mp = new HashMap<String, Integer>();
        
        //计算自由度并筛选出自由度符合要求的
        for (String s : PreValidWords) {
            if (!(RDOF.containsKey(s) && LDOF.containsKey(s))) continue;
            double L = LDOF.get(s);
            double R = RDOF.get(s);
            double DofVal = Math.min(L, R);
            if (DofVal > MinDof) {
                mp.put(s, FreqencyMap.get(s));
            }
        }
        
        //返回key-value对
        return mp;   
        
    }
    
    
    
    private Map<String, Double> calculateDoc(Map<String, Integer> FreqencyMap) {
        
        Map<String, Double> Doc = new HashMap<String, Double> ();
        for (String s : FreqencyMap.keySet()) {
            int len = s.length();
            if (len < 2) continue;
            double docVal = 1e20;
            int cnt = FreqencyMap.get(s);
            for (int i = 1; i < len; ++i) {
                String LS = s.substring(0, i);
                String RS = s.substring(i, len);
                int Lcnt = FreqencyMap.get(LS);
                int Rcnt = FreqencyMap.get(RS);
                docVal = Math.min(docVal, 1.0 * cnt * totalLen / (1.0 * Lcnt * Rcnt));
            }
            if (docVal < MinDoc) 
                continue;
            else {
                Doc.put(s, docVal);
            }
        }
        return Doc;
        
    }
    
    
    
    
    
    private Map<String, Double> DOF(Set<String> PreValidWords, Map<String, Integer> Freqency, List<String> postfixs, boolean reverse) {
        
        String lastWord = new String("");
        char lastChar = 'a';
        int lastCharCount = 1;
        int wordFreqency = 1;
        double DofVal = 0;
        
        Map<String, Double> Dof = new HashMap<String, Double>();
        
        for (int len = 2; len <= wordLen; ++len) {
            for (String str : postfixs) {
                StringBuilder sb = new StringBuilder(str.substring(0, len));
                if (reverse) {
                    sb.reverse();
                }
                String s = new String(sb);
                if (!PreValidWords.contains(s)) continue;
                if (s.equals(lastWord)) {
                    if (str.charAt(len) == lastChar) {
                        lastCharCount++;
                    } else {
                        DofVal += (double)lastCharCount / wordFreqency * Math.log((double)lastCharCount / wordFreqency);
                        lastCharCount = 1;
                        lastChar = str.charAt(len);
                    }
                } else {
                    DofVal += (double)lastCharCount / wordFreqency * Math.log((double)lastCharCount / wordFreqency);
                    Dof.put(lastWord, -DofVal);
                    lastCharCount = 1;
                    DofVal = 0;
                    lastChar = str.charAt(len); 
                    lastWord = s;
                    wordFreqency = Freqency.get(s);
                }
            }
        }
        DofVal += (double)lastCharCount / wordFreqency * Math.log((double)lastCharCount / wordFreqency);
        Dof.put(lastWord, DofVal);
        
        return Dof;
    }
    
    
    
    
    
    private List<String> buildPostfixList(String str) {
        ArrayList<String> postfixs = new ArrayList<String>();
        
        for (int i = 0; i < totalLen; ++i) {
            postfixs.add(new String(str.substring(i, i + wordLen + 1)));
        }
        
        Collections.sort(postfixs);
        
        return postfixs;
    }
    
    
    
    
    private Map<String, Integer> CountAllFreqency(List<String> postfixs) {
        
        Map<String, Integer> fre = new HashMap<String, Integer>();
        
        String lastString = new String("");
        int lastStringCnt = 0;
        for (int len = 1; len <= wordLen; ++len) {
            for (String S : postfixs) {
                String s = S.substring(0, len);
                if (s.equals(lastString)) {
                    lastStringCnt++;
                } else {               
                    if (lastStringCnt >= MinOccur) 
                        fre.put(lastString, lastStringCnt);
                    lastString = s;
                    lastStringCnt = 1;
                }
            }
        }
        if (lastStringCnt >= MinOccur) 
            fre.put(lastString, lastStringCnt);
        fre.remove("");
        
        return fre;
    }
    
    
    public Iterable<Tuple2<String, Integer>> call(Iterator<String> ads) {
        
        ArrayList<String> adList = new ArrayList<String> ();
        
        while (ads.hasNext()) {
            String ad = ads.next();
            adList.add(ad);
        }
        
        Map<String, Integer> wfMap = MainFunction(adList);
        
        List<Tuple2<String, Integer>> resultList = new ArrayList<Tuple2<String, Integer>>();
        
        for (String word : wfMap.keySet()) {
            resultList.add(new Tuple2<String, Integer>(word, wfMap.get(word)));
        }
        
        return resultList;
         
    } 
    
    
    private static void printHeapMemoryInfo(String str) {
        System.out.println(str);
        Runtime runtime = Runtime.getRuntime();  
        System.out.printf("maxMemory : %.2fM\n", runtime.maxMemory()*1.0/1024/1024);  
        System.out.printf("totalMemory : %.2fM\n", runtime.totalMemory()*1.0/1024/1024);  
        System.out.printf("freeMemory : %.2fM\n", runtime.freeMemory()*1.0/1024/1024);  
        System.out.printf("usedMemory : %.2fM\n", (runtime.totalMemory()-runtime.freeMemory())*1.0/1024/1024);  
    } 
}
