# HyperNeptune用户手册

您好，欢迎使用 `Neptune` 数据库系统！

`Neptune` 得名于太阳系的最后一颗行星——海王星。这是一个简易的 `OLTP` 数据库系统，适合用于执行简单的增删查改业务逻辑。

`Neptune` 使用 `Java` 语言编写，用于学习和研究数据库系统原理，而非用于商业和生产环境，所以较不稳定，未经充分验证与测试，请不要将它用于实际场合。作者不为程序所造成的一切后果负责。假如读者对本数据库的使用或实现有一些兴趣，请联系作者 `<824466875 AT qq DOT com>`。

作者放弃 `Neptune` 的版权，因此，使用这些代码片段只需要遵守其中部分的 `Apache` 协议。 

## 安装与运行

### **运行环境**

要运行这个数据库，用户需要在电脑上配备 1.8 版本的 Java 运行时环境。这个版本的 Java 又叫做 Java 8.

### 程序启动

#### 服务端  `ThssDB`

* 运行 `thssdb` 的 `jar` 包即可。

#### 客户端  `Client`

* 运行客户端的 `jar` 包即可。


## 数据库管理

在与服务器成功连接后，才可执行`SQL`语句。因为预算和时间限制，我们没有实现用户系统。所以，用户可以随便输入一个用户名和密码来登录。像这样：
```sql
connect 1 1;
```

我们的数据库是一个三级结构，一个数据库系统实例下面有多个数据库，用户在数据库中管理所有的关系表。因此，要想使用这个数据库系统，需要先创建和使用数据库，相关的语法如下所示。
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

## 关系

在使用数据库之后，我们就可以在数据库中使用 `SQL` 语言定义、修改和查询关系了。

默认我们会将每一个查询的语句作为一个事务，并且 `autocommit`，用户也可以使用 `begin transaction` 和 `commit` 语句手写一个事务。

注意 `begin transaction` 后面必须没有分号，而 `commit` 的后面一定要有分号，也就是说写成 `commit;` 的形式。

### 创建表

主键只支持一列。

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
SELECT  attrName1, attrName2, ..., attrNameN  FROM  tableName  WHERE  attrName1 = attrValue...;
SELECT  attrName1,..., tableName1.attrName1,...  FROM  tableName1 JOIN tableName2  ON  tableName1.attrName1 = tableName2.attrName2 WHERE  attrName1 = attrValue...;
```

上述语句中，`WHERE`子句支持多重`and/or`，并且关系为`<,>,<>,>=,<=,=,IS NULL`之一。

`SELECT`子句包括`[tableName.]attrName,tableName.*,*` 以及 `DISTINCT/ALL`。

`JOIN` 支持多表的连接，但是只能以 `FROM` 中有多张表的形式连接超过两张以上的表，`JOIN` 只能写一次，两张表。
