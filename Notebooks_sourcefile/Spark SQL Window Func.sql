-- Databricks notebook source
CREATE TABLE employees
   (name STRING, dept STRING, salary INT, age INT);

-- COMMAND ----------

INSERT INTO employees
   VALUES ('Lisa', 'Sales', 10000, 35),
          ('Evan', 'Sales', 32000, 38),
          ('Fred', 'Engineering', 21000, 28),
          ('Alex', 'Sales', 30000, 33),
          ('Tom', 'Engineering', 23000, 33),
          ('Jane', 'Marketing', 29000, 28),
          ('Jeff', 'Marketing', 35000, 38),
          ('Paul', 'Engineering', 29000, 23),
          ('Chloe', 'Engineering', 23000, 25);

-- COMMAND ----------

SELECT name, dept, salary, age 
FROM employees;

-- COMMAND ----------

SELECT name,
         dept,
         RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank
  FROM employees;

-- COMMAND ----------

SELECT name,
         dept,
         DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dense_rank
    FROM employees;

-- COMMAND ----------

SELECT name,
         dept,
         age,
         CUME_DIST() OVER (PARTITION BY dept ORDER BY age
                           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_dist
    FROM employees;

-- COMMAND ----------

SELECT name,
         dept,
         salary,
         MIN(salary) OVER (PARTITION BY dept ORDER BY salary) AS min
    FROM employees;

-- COMMAND ----------

SELECT name,
         salary,
         LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS lag,
         LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary) AS lead
    FROM employees;

-- COMMAND ----------


