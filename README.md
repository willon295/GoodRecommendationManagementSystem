# GoodRecommendationManagementSystem

基于Hadoop简单商品推荐引擎

## 目的

根据不同用户之间的购买行为，向用户推荐未购买的商品

## 原始数据

`matrix.txt`:

|用户id|商品id|是否购买|
|---|---|---|
|10001  | 20001  | 1|
|10001  | 20002  | 1|
|10001  | 20005  | 1|
|10001  | 20006  | 1|
|10001  | 20007  | 1|
|10002  | 20003  | 1|
|10002  | 20004  | 1|
|10002  | 20006  | 1|
|10003  | 20002  | 1|
|10003  | 20007  | 1|
|10004  | 20001  | 1|
|10004  | 20002  | 1|
|10004  | 20005  | 1|
|10004  | 20006  | 1|
|10005  | 20001  | 1|
|10006  | 20004  | 1|
|10006  | 20007  | 1|

## 最终数据

写入 `MySQL` 数据库， `exp` : 推荐度

```
+-------+-------+------+
| uid   | gid   | exp  |
+-------+-------+------+
| 10001 | 20003 |    1 |
| 10001 | 20004 |    2 |
| 10002 | 20001 |    2 |
| 10002 | 20002 |    2 |
| 10002 | 20005 |    2 |
| 10002 | 20007 |    2 |
| 10003 | 20001 |    3 |
| 10003 | 20004 |    1 |
| 10003 | 20005 |    3 |
| 10003 | 20006 |    3 |
| 10004 | 20003 |    1 |
| 10004 | 20004 |    1 |
| 10004 | 20007 |    5 |
| 10005 | 20002 |    2 |
| 10005 | 20005 |    2 |
| 10005 | 20006 |    2 |
| 10005 | 20007 |    1 |
| 10006 | 20001 |    1 |
| 10006 | 20002 |    2 |
| 10006 | 20003 |    1 |
| 10006 | 20005 |    1 |
| 10006 | 20006 |    2 |
+-------+-------+------+

```

## 基于用户

用户购买的物品重合度高，那么用户相似度比较高。 相似度高的用户之间进行物品推荐


## 基于物品

物品的相似度： 物品的共现次数
物品同时被用户购买的次数，即销量接近，我们认为这些物品之间的相似度高。给用户推荐相似度高的物品 


# 步骤

1. 计算用户的购买列表
```
用户  物品1,物品2,物品3...
```
2. 生成物品的共现关系
```
物品1  物品2
物品1  物品3
....
```
3. 计算共现次数
```
物品 物品1:次数，物品2：次数,物品3：次数...
```
4. 计算用户的购买向量
源数据：可以使第一步的结果，可以使最初的数据，结果数据
```
物品1  用户1:1,用户2:1
```
5. 物品的共现次数矩阵×用户的购买向量=零散推荐结果
6. 统计零散的推荐结果 ，即求和
7. 推荐结果数据 和 原始数据进行比较， 去掉用户已经购买的商品
8. 最终推荐结果写入 `MySQL` 数据库
9. 创建作业流 ，调度 1-8 
