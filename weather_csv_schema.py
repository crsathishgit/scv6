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
# extra_column_validation = [CustomElementValidation(lambda e:  extraColumn(e), 'this field cannot be null')]

weatherCsvSchema = [
  {
    "columnName": 'MinTemp',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'MaxTemp',
    "validations": decimal_validation
  },
  {
    "columnName": 'Rainfall',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Evaporation',
    "validations": decimal_validation
  },
  {
    "columnName": 'Sunshine',
    "validations": decimal_validation
  },
  {
    "columnName": 'WindGustDir',
    "validations": null_validation
  },
  {
    "columnName": 'WindGustSpeed',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'WindDir9am',
    "validations": null_validation
  },
  {
    "columnName": 'WindDir3pm',
    "validations": null_validation
  },
  {
    "columnName": 'WindSpeed9am',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'WindSpeed3pm',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Humidity9am',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Humidity3pm',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Pressure9am',
    "validations": decimal_validation
  },
  {
    "columnName": 'Pressure3pm',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Cloud9am',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Cloud3pm',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'Temp9am',
    "validations": decimal_validation
  },
  {
    "columnName": 'Temp3pm',
    "validations": decimal_validation
  },
  {
    "columnName": 'RainToday',
    "validations": null_validation + none_validation
  },
  {
    "columnName": 'RISK_MM',
    "validations": decimal_validation
  },
  {
    "columnName": 'RainTomorrow',
    "validations": null_validation + none_validation
  }
]


def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True


def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True

def checkNone(n):
    if(n == None):
      return False
    print(str(n) + ":  " + str(type(n)))
    # if(np.isnan(n)):
    #     return False
    return True

def getValidations():
    clmn_validations = []
    for item in weatherCsvSchema:
      clmn_validations.append(Column(item["columnName"], item["validations"]))
    return clmn_validations

def getSchema():
    # define validation schema
    schema = pandas_schema.Schema(getValidations())
    return schema

def getColumnsCount():
  return len(weatherCsvSchema)

def getColumnsHeader(delimiter):
    header = ""
    for item in weatherCsvSchema:
      header += item["columnName"] + delimiter
    return header + "\n"

def getColumnNames():
  columns = []
  for item in weatherCsvSchema:
    columns.append(item["columnName"])
  return columns
