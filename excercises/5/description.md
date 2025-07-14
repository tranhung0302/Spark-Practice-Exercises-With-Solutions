### Exercise 5: Structs for column names and values

Write a structured query that “transpose” a dataset so a new dataset uses column names and values from a struct column.

Input:
```
+------+--------------------------------------------------+
|name  |movieRatings                                      |
+------+--------------------------------------------------+
|Manuel|[[Logan, 1.5], [Zoolander, 3.0], [John Wick, 2.5]]|
|John  |[[Logan, 2.0], [Zoolander, 3.5], [John Wick, 3.0]]|
+------+--------------------------------------------------+
```

Result:
```
+------+-----+---------+---------+
|name  |Logan|Zoolander|John Wick|
+------+-----+---------+---------+
|Manuel|1.5  |3.0      |2.5      |
|John  |2.0  |3.5      |3.0      |
+------+-----+---------+---------+
```
