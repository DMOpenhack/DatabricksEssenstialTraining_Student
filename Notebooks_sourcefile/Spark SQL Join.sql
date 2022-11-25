-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Use employee and department tables to demonstrate different type of joins
-- MAGIC 
-- MAGIC [Source](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-join.html)

-- COMMAND ----------

drop view if exists employee;

CREATE TEMP VIEW employee(id, name, deptno) AS
     VALUES(105, 'Chloe', 5),
           (103, 'Paul' , 3),
           (101, 'John' , 1),
           (102, 'Lisa' , 2),
           (104, 'Evan' , 4),
           (106, 'Amy'  , 6);
           
drop view if exists department;

CREATE TEMP VIEW department(deptno, deptname) AS
    VALUES(3, 'Engineering'),
          (2, 'Sales'      ),
          (1, 'Marketing'  );           

-- COMMAND ----------

-- Use employee and department tables to demonstrate inner join.
SELECT id, name, employee.deptno, deptname
   FROM employee
   INNER JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate left join.
SELECT id, name, employee.deptno, deptname
   FROM employee
   LEFT JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate right join.
SELECT id, name, employee.deptno, deptname
    FROM employee
    RIGHT JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate full join.
SELECT id, name, employee.deptno, deptname
    FROM employee
    FULL JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate cross join.
SELECT id, name, employee.deptno, deptname
    FROM employee
    CROSS JOIN department;

-- COMMAND ----------

-- Use employee and department tables to demonstrate semi join.
SELECT *
    FROM employee
    SEMI JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate anti join.
SELECT *
    FROM employee
    ANTI JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate lateral inner join.
SELECT id, name, deptno, deptname
    FROM employee
    JOIN LATERAL (SELECT deptname
                    FROM department
                    WHERE employee.deptno = department.deptno);

-- COMMAND ----------

-- Use employee and department tables to demonstrate lateral left join.
SELECT id, name, deptno, deptname
    FROM employee
    LEFT JOIN LATERAL (SELECT deptname
                         FROM department
                         WHERE employee.deptno = department.deptno);
