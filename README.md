Bitcask
=======

http://blog.nosqlfan.com/html/955.html<br/>
http://downloads.basho.com/papers/bitcask-intro.pdf<br/>

采用Bitcask存储模型<br/>
1. 顺序写，随机读<br/>
2. 采用变长编码,大大节约内存空间,抛弃了论文中的TimeStamp<br/>
3. 支持多线程<br/><br/><br/>

Benchmark<br/><br/>
key int 类型 4个字节<br/>
value 1--2000个长度的随机字符串<br/>

put 1000000 ,cost 45.656 ms<br/>
remove 1000000 cost 12.818 ms<br/><br/><br/>


下一步计划<br/>
1.随机读增加 LRU cache<br/>
2.增加复制功能,支持HA<br/>
...
