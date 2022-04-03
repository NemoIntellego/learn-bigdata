

## 作业1

展示电影 ID 为 2116 这部电影各年龄段的平均影评分


```
SELECT u.age age, avg(r.rate) avgrate FROM t_rating r JOIN t_user u ON r.userid = u.userid WHERE r.movieid = 2116 GROUP BY u.age;
```

执行结果如下：

```
1	3.2941176470588234
18	3.3580246913580245
25	3.436548223350254
35	3.2278481012658227
45	2.8275862068965516
50	3.32
56	3.5
```


## 作业2

找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数

```
SELECT u.sex sex, m.moviename name, avg(r.rate) avgrate, count(*) total FROM t_user u JOIN t_rating r ON u.userid = r.userid JOIN t_movie m ON r.movieid = m.movieid WHERE u.sex = 'M' GROUP BY u.sex, m.moviename HAVING total >= 50 ORDER BY avgrate DESC LIMIT 10;
```

执行结果如下：

```
M	Sanjuro (1962)	4.639344262295082	122
M	Godfather, The (1972)	4.583333333333333	3480
M	Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)	4.576628352490421	1044
M	Shawshank Redemption, The (1994)	4.560625	3200
M	Raiders of the Lost Ark (1981)	4.520597322348094	3884
M	Usual Suspects, The (1995)	4.518248175182482	2740
M	Star Wars: Episode IV - A New Hope (1977)	4.495307167235495	4688
M	Schindler's List (1993)	4.49141503848431	3378
M	Paths of Glory (1957)	4.485148514851486	404
M	Wrong Trousers, The (1993)	4.478260869565218	1288
```

## 作业3

找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）

说明：影评次数最多的女士，该女士给出最高分的电影超过10部，所以按照movieid升序找出这10部。

```
SELECT m.moviename name, avg(r.rate) avgrate FROM t_rating r JOIN t_movie m ON r.movieid = m.movieid JOIN (SELECT r.movieid FROM t_rating r JOIN (SELECT u.userid, count(*) num FROM t_user u JOIN t_rating r ON u.userid = r.userid WHERE u.sex = 'F' GROUP BY u.userid ORDER BY num DESC LIMIT 1) u ON r.userid = u.userid ORDER BY r.rate DESC, movieid LIMIT 10 ) mm on r.movieid = mm.movieid GROUP BY m.moviename;
```

执行如下：

```
Star Wars: Episode IV - A New Hope (1977)	4.453694416583082
Death and the Maiden (1994)	3.5
Living in Oblivion (1995)	3.9170731707317072
Professional, The (a.k.a. Leon: The Professional) (1994)	4.106175514626218
Swimming with Sharks (1995)	3.712918660287081
Three Colors: Red (1994)	4.227544910179641
Crumb (1994)	4.063136456211812
Shallow Grave (1994)	3.8937728937728937
Three Colors: Blue (1993)	4.098765432098766
City of Lost Children, The (1995)	4.062034739454094
```