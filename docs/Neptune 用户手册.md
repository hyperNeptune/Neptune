# HyperNeptune用户手册

您好，欢迎使用 `Neptune` 数据库系统！

`Neptune` 得名于太阳系的最后一颗行星——海王星。这是一个简易的 `OLTP` 数据库系统，适合用于执行简单的增删查改业务逻辑。

`Neptune` 使用 `Java` 语言编写，用于学习和研究数据库系统原理，而非用于商业和生产环境，所以较不稳定，未经充分验证与测试，请不要将它用于实际场合。作者不为程序所造成的一切后果负责。假如读者对本数据库的使用或实现有一些兴趣，请联系作者 `<824466875 AT qq DOT com>`。

作者放弃 `Neptune` 的版权，因此，使用这些代码片段只需要遵守其中部分的 `Apache` 协议。 

## 安装与运行

#### **运行环境**

**操作系统：**Windows 11

**IDE:**  IDEA

**JRE版本：**1.8

#### 程序启动

##### 服务端  `ThssDB`

* 运行`cn.edu.thssdb.server.ThssDB`主类以运行服务端程序。
* 默认命令行参数：
  * `java C`

##### 客户端  `Client`

* 运行`cn.edu.thssdb.client.Client`主类以运行客户端程序。
* 默认命令行参数：

  - `java -h 127.0.0.1 -p 6667`

#### 连接与断开（客户端）

##### 用户连接

* 启动客户端后，输入如下命令，并输入用户名/密码以连接到服务端：
  * **ThssDB>**`connect;`
  * **Username:**`<username>`
  * **Password:**`<password>`
* 内建的管理权限用户名与密码：username/password

##### 断开连接

* 在已于服务端连接的情况下，输入如下命令以断开连接：

  * **ThssDB>**`disconnect;`

## SQL 语法

* 仅在与服务器成功连接后，才可执行`SQL`语句。
* 执行`SQL`语句，需要先输入`execute`命令，而后再输入待执行`SQL`语句：
  * **ThssDB>**`execute;`
  * **Statement:**`<statement>`

### 创建用户

```sql
CREATE USER username IDENTIFIED BY password
```

### 创建数据库

```sql
CREATE DATABASE databaseName
```

### 删除数据库

```sql
DROP DATABASE databaseName
```

### 使用数据库

```sql
USE databaseName
```

### 创建表

```sql
CREATE TABLE tableName(
    attrName1 Type1, 
    attrName2 Type2,
    attrNameN TypeN NOT NULL,     
    …, 
    PRIMARY KEY(attrName1)
);
```

### 删除表

```sql
DROP TABLE tableName
```

### 展示表(展示元数据)

```sql
SHOW TABLE tableName
```

### 插入

一次可以插入多行（ `VALUES` 后可以有多个值）

```sql
INSERT INTO tableName(attrName1, attrName2,..., attrNameN) VALUES (attrValue1, attrValue2,..., attrValueN), ...
```

### 删除

```sql
DELETE  FROM  tableName  WHERE  attrName = attrValue ...
```

### 更改

```sql
UPDATE  tableName  SET  attrName = attrValue  WHERE  attrName = attrValue ...
```

### 查询

```sql
SELECT  attrName1, attrName2, ..., attrNameN  FROM  tableName  WHERE  attrName1 = attrValue ... ORDER BY attrName1, ... [DESC/ASC]
SELECT  attrName1,..., tableName1.attrName1,...  FROM  tableName1 JOIN tableName2  ON  tableName1.attrName1 = tableName2.attrName2 WHERE  attrName1 = attrValue ... ORDER BY attrName1, ... [DESC/ASC]
```

上述语句中，`WHERE`子句支持多重`and/or`，并且关系为`<,>,<>,>=,<=,=,IS NULL`之一。

`SELECT`子句包括`[tableName.]attrName,tableName.*,*,[tableName.]attrName OP CONST,CONST OP [tableName.]attrName, CONST OP CONST`以及五种聚集函数（`avg,sum,min,max,count`）、`DISTINCT/ALL`关键字，其中`OP`为加减乘除，`CONST`为常数。

`JOIN`子句包括`INNER JOIN/JOIN/NATURAL JOIN/,(笛卡尔积)/LEFT OUTER JOIN/RIGHT OUTER JOIN/FULL OUTER JOIN`，至多涉及2张表，`ON` 子句支持多重`and`。

`ORDER BY`子句支持多列排序，以及`DESC/ASC`关键字。

### 开始事务 ？

```sql
BEGIN TRANSACTION
```

### 提交

```sql
COMMIT
```

### 保存点

```sql
SAVEPOINT savepointName
```

### 回滚

```sql
ROLLBACK [TO SAVEPOINT savepointName]
```

### 检查点

```sql
CHECKPOINT
```
