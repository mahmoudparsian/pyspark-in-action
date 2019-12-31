#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame 
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
# import pyspark class Row from module sql
from pyspark.sql import Row

#================================================
# Row is a class defined in pyspark.sql module:
#
# class pyspark.sql.Row
# A row in DataFrame. The fields in it can be accessed:
#
# like attributes (row.key)
# like dictionary values (row[key])
# key in row will search through row keys.
# 
# Row can be used to create a row object by 
# using named arguments, the fields will be 
# sorted by names. It is not allowed to omit 
# a named argument to represent the value is 
# None or missing. This should be explicitly 
# set to None in this case.
#
# Examples:
#
# >>> row = Row(name="Alex", age=33, city="Sunnyvale")
# >>> row
# Row(age=33, city="Sunnyvale", name="Alex")
# >>> row['name'], row['age']
# ('Alex', 33)
# >>> row.name, row.age
# ('Alex', 33)
# >>> 'name' in row
# True
# >>> 'wrong_key' in row
# False
#
# Row also can be used to create another Row 
# like class, then it could be used to create 
# Row objects, such as
#
# >>> Person = Row("name", "age")
# >>> Person
# <Row(name, age)>
# >>> 'name' in Person
# True
# >>> 'wrong_key' in Person
# False
# >>> Person("Jane", 24)
# Row(name='Jane', age=24)
#================================================


if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_from_rows.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_rows")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # input_path = sys.argv[1]  
    # print("input_path: {}".format(input_path))


    # DataFrames Creation from pyspark.sql.Row

    # DataFrames can be created from Row(s).

    # Example: Create Departments and Employees

    # Create the Departments
    dept1 = Row(id='100', name='Computer Science')
    dept2 = Row(id='200', name='Mechanical Engineering')
    dept3 = Row(id='300', name='Music')
    dept4 = Row(id='400', name='Sports')
    dept5 = Row(id='500', name='Biology')

    # Create the Employees
    Employee = Row("first_name", "last_name", "email", "salary")
    #
    employee1 = Employee('alex', 'smith', 'alex@berkeley.edu', 110000)
    employee2 = Employee('jane', 'goldman', 'jane@stanford.edu', 120000)
    employee3 = Employee('matei', None, 'matei@yahoo.com', 140000)
    employee4 = Employee(None, 'eastwood', 'jimmy@berkeley.edu', 160000)
    employee5 = Employee('betty', 'ford', 'betty@gmail.com', 130000)

    # Create the DepartmentWithEmployees instances from Departments and Employees
    department_with_employees_1 = Row(department=dept1, employees=[employee1, employee2, employee5])
    department_with_employees_2 = Row(department=dept2, employees=[employee3, employee4])
    department_with_employees_3 = Row(department=dept3, employees=[employee1, employee4])
    department_with_employees_4 = Row(department=dept4, employees=[employee2, employee3])
    department_with_employees_5 = Row(department=dept5, employees=[employee5])

    #
    print ("dept1=", dept1)
    print ("dept5=", dept5)
    #
    print ("employee2=", employee2)
    print ("employee4=", employee4)
    print ('department_with_employees_1.employees[0].email', department_with_employees_1.employees[0].email)

    #==========================================
    # Create DataFrames from a list of the rows
    #==========================================
    ## simplify
    # departments_with_employees_seq = [eval('department_with_employees_{}'.format(i)) for i in range(1, 6)]
    departments_with_employees_seq_1 = [department_with_employees_1, department_with_employees_2, department_with_employees_5]
    df = spark.createDataFrame(departments_with_employees_seq_1)
    #
    df.show(truncate=False)
    df.printSchema()
    
    
    departments_with_employees_seq_2 = [department_with_employees_1, department_with_employees_3, department_with_employees_4, department_with_employees_5]
    df2 = spark.createDataFrame(departments_with_employees_seq_2)

    #
    df2.show(truncate=False)
    df2.printSchema()


    # done!
    spark.stop()
