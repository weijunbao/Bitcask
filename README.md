Bitcask
=======

http://blog.nosqlfan.com/html/955.html<br/>
http://downloads.basho.com/papers/bitcask-intro.pdf<br/>

采用Bitcask存储模型<br/>
1. 顺序写，随机读<br/>
2. 采用变长编码,大大节约内存空间,抛弃了论文中的TimeStamp<br/>
3. 支持多线程<br/><br/><br/>


下一步计划<br/>
1.随机读增加 LRU cache<br/>
2.增加复制功能,支持HA<br/>
...
