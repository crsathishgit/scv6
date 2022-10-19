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


csvSchema = [
  {
    "columnName": 'Experiment ',
    "validations": null_validation
  },
  {
    "columnName": 'Virus ',
    "validations": null_validation
  },
  {
    "columnName": 'Cell',
    "validations": null_validation
  },
  {
    "columnName": 'MOI',
    "validations": int_validation + none_validation
  },
  {
    "columnName": 'hpi',
    "validations": decimal_validation
  },
  {
    "columnName": 'Titer',
    "validations": decimal_validation + none_validation
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
    if(np.isnan(n)):
        return False
    return True

def getValidations():
    clmn_validations = []
    for item in csvSchema:
      clmn_validations.append(Column(item["columnName"], item["validations"]))
    return clmn_validations

def getSchema():
    # define validation schema
    schema = pandas_schema.Schema(getValidations())
    return schema

def getColumnsCount():
  return len(csvSchema)