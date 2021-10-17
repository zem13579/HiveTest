DROP DATABASE IF EXISTS employees2;
CREATE DATABASE IF NOT EXISTS employees2;
USE employees2;


DROP TABLE IF EXISTS dept_emp;
DROP TABLE IF EXISTS dept_manager;
DROP TABLE IF EXISTS titles;
DROP TABLE IF EXISTS salaries;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS departments;

CREATE TABLE employees (
                           emp_no      string ,
                           birth_date  string ,
                           first_name  string ,
                           last_name   string ,
                           gender      string ,
                           hire_date   string
);

CREATE TABLE departments (
                             dept_no     string        ,
                             dept_name   string

);

CREATE TABLE dept_manager (
                              emp_no       string            ,
                              dept_no      string        ,
                              from_date    string           ,
                              to_date      string

);

CREATE TABLE dept_emp (
                          emp_no      string        ,
                          dept_no     string        ,
                          from_date   string        ,
                          to_date     string

);

CREATE TABLE titles (
                        emp_no      string    ,
                        title       string    ,
                        from_date   string    ,
                        to_date     string

)
;

CREATE TABLE salaries (
                          emp_no      string      ,
                          salary      string      ,
                          from_date   string      ,
                          to_date     string

)
;
