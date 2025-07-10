### Exercise 2: Selecting the most important rows per assigned priority

Write a structured query that selects the most important rows per assigned priority.

Module: Spark SQL

Input:
```
+---+------+
| id| value|
+---+------+
|  1|   MV1|
|  1|   MV2|
|  2|   VPV|
|  2|Others|
+---+------+
```

Result:
```
+---+----+
| id|name|
+---+----+
|  1| MV1|
|  2| VPV|
+---+----+
```