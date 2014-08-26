FindWordsonSpark
================

A small project to create a words list of some texts which is running on Spark.


抽词算法:

算法理论详见matrix67博客: http://www.matrix67.com/blog/archives/5044
如matrix67博客所讲,抽词算法主要用的是两个指标,凝固度与自由度.

凝固度: 例如"百姓"一词,假设文本长10000,"百"出现4次, "姓"出现2次, 那么"百""姓"同时出现在一起概念为p("百") * p("姓") = 4 / 10000 * 2 / 10000 =  8/100000000, 那么"百姓"出现一次,p("百姓") = 1/10000 >> p("百") * p("姓"), 故极有可能成词.
自由度:例如"捷克斯洛伐克"一词,"克斯诺伐克"一词显然凝固度很高,然而不是词,于是主要要看词的左右两端再出现的字是否足够灵活. 比如"克斯诺伐克"左边几乎100%概率出现"捷",于是自由度超低,无法成词.

一个词之所以成词,就是又要凝固度高又要自由度高

另一个指标是出现频率,频率越高,越是热词,另一方面,频率出现很低很低的词也许就不是我们关心的了.


当然,但要抽词的文本巨大的时候,单机跑不动,于是要上集群上跑,分而治之.在spark上跑的时候算法大概是这样的.

首先在之前的单机抽词中,用的工程车5000条，资本论,西游记分别是900K, 3.4m和2m,在这样的文本里已经能够把热词抽出来,其实只要文本大于兆这个级别,完全可以实现抽词,另一方面,如果这个所有语料同时考虑用来抽词,需要的内存相当大(单机1m文本用了100m内存, 如果文本1G必然崩溃).


于是,算法的核心想法就是,在spark处理分块时,我大概以5m为一个parition,并行的对各个partition里5m的文本进行抽词,得到key-value(词-词频)对,然后各个partition得到的(key-value)进行reduce操作.得到所有的key-value(词-词频)对.

 ![image](https://github.com/wh61/FindWordsonSpark/master/img/FindWordsonSpark.png)

在抽词的时候,设定了几个参数,最小频数,最小凝固度和最小自由度,一个词要超过最小凝固度和最小自由度这个两个条件,才能算是一个词,而他要超过最小频数,才会使我们所关心的词.
因为在抽词过程中要用到许多map(候选词-词频, 候选词-左(右)自由熵, 候选词-凝固度),这些满满的都是时间空间的消耗,于是算法实现过程中一步一步剪枝下来的.

满足最小频率的词(计算凝固度)->满足最小凝固度的词(计算自由度)->满足最小自由度的词(结果,等着reduce).

详细见算法源代码注释.
