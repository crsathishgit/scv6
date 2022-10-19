import json
import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
import numpy as np
from decimal import *

  # define validation elements
decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'decimal_validation: is not decimal')]
int_validation = [CustomElementValidation(lambda i: check_int(i), 'int_validation: is not integer')]
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'null_validation: this field cannot be null')]
none_validation = [CustomElementValidation(lambda n:  checkNone(n), 'checkNone: this field cannot be null')]
date_validation = [CustomElementValidation(lambda d:  check_date(d), f'check_date: Date not valid')]
dummy_Validation = [CustomElementValidation(lambda d:  dummyValidation(d), 'dummy validation')]
# extra_column_validation = [CustomElementValidation(lambda e:  extraColumn(e), 'this field cannot be null')]

csvSchema = []

def check_date(d):
    try:
      if(len(d.split("-")) == 3 or len(d.split("/")) == 3):
        return True
      else:
        return False
    except:
      return False

def dummyValidation(d):
  return True

def check_decimal(dec):
    # try:
    #     Decimal(dec)
    # except InvalidOperation:
    #     return False
    return True


def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True

def checkNone(n):
    # if(n == None):
    #   return False
    # print(str(n) + ":  " + str(type(n)))
    # if(np.isnan(n)):
    #     return False
    return True

def getValidations(columnsFromJSON):
    global csvSchema
    clmn_validations = []
    print("in getValidations ----> start")
    validationMethod = ""
    for column in columnsFromJSON:
        if(column["mandatory"] and column["mandatory"] == True):
          if(column["datatype"] == "integer"):
            validationMethod = int_validation 
          elif(column["datatype"] == "decimal"):
            validationMethod = decimal_validation 
          elif(column["datatype"] == "string"):
            validationMethod = null_validation
          elif(column["datatype"] == "date"):
            validationMethod = date_validation
          clmn_validations.append(Column(column["columnName"], validationMethod))
        else:
          validationMethod = dummy_Validation
          clmn_validations.append(Column(column["columnName"], validationMethod))
        csvSchema.append({
          "columnName": column["columnName"],
          "validations": validationMethod
        })


    print("in getValidations ----> end")
    print(csvSchema)
    # for item in csvSchema:
    #   clmn_validations.append(Column(item["columnName"], item["validations"]))
    return clmn_validations

def getSchema(columnsFromJSON):
    # define validation schema
    schema = pandas_schema.Schema(getValidations(columnsFromJSON))
    return schema

def getColumnsCount():
  return len(csvSchema)

def getColumnsHeader(delimiter):
    header = ""
    for item in csvSchema:
      header += item["columnName"] + delimiter
    return header + "\n"

def getColumnNames():
  columns = []
  for item in csvSchema:
    columns.append(item["columnName"])
  return columns
