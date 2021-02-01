'''
    AUTHOR: HUSNUL
'''

from mt940 import MT940
from datetime import datetime
import pandas as pd
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
    
def get_dict(transaction):
    
    '''
        define dictionary for datafame
        
        transaction details containing: transaction_date + CREDIT/DEBIT + transaction_amount + transaction_id
    '''
    
    date = datetime.strptime(str(transaction.date), '%Y-%M-%d').strftime('%y%M%d')
    c_or_d = 'D' if '-' in str(transaction.amount) else 'C'
    transaction_details = date+c_or_d+str(transaction.amount).replace('-','')+str(transaction.id)
    dictionary = {
        'transaction_details': transaction_details,
        'date': str(transaction.date),
        'booking': transaction.booking,
        'amount': str(transaction.amount),
        'id': transaction.id,
        'reference': transaction.reference,
        'additional_data': transaction.additional_data,
        'transaction_description': transaction.description
    }
    
    return dictionary
    
	
def get_dataframe():
    
    '''
        build pandas dataframe
    '''
    
    # count total transaction
    mt940 = MT940('capital.TXT')
    statement = mt940.statements[0]
    count = len(statement.transactions)
    
    # loop in total transaction returning list
    transactions = [ statement.transactions[i] for i in range(count) ]
    
    # loop in list to get its dictionary
    a = [get_dict(i) for i in transactions]
    
    df = pd.DataFrame(a)
    
    return df
	
def toSpark(spark):
    ppn_dttm = datetime.now().strftime('%Y%m%d%H%M%S')
    
    pandas_df = get_dataframe()
    schema = StructType([
        StructField('transaction_details', StringType(), True),
        StructField('date', StringType(), True),
        StructField('booking', StringType(), True),
        StructField('amount', StringType(), True),
        StructField('id', StringType(), True),
        StructField('reference', StringType(), True),
        StructField('additional_data', StringType(), True),
        StructField('transaction_description', StringType(), True)
    ])
    
    df = spark.createDataFrame(pandas_df, schema=schema)
    df.createOrReplaceTempView('tmp')
    df = spark.sql(f'''
        SELECT
            transaction_details,
            if(substr(transaction_details,7,1)='C', 'Credit','Debit'),
            amount,
            id code,
            transaction_description,
            REGEXP_REPLACE(regexp_extract(transaction_description, '\\\s([^\\\s]+)$', 1),'[^0-9]+', "") va,
            date txn_date
        FROM
            tmp''')
    df.show()

def main():
    spark = SparkSession.builder.appName('mt940')\
            .config('hive.exec.dynamic.partition.mode','nonstrict')\
            .enableHiveSupport().getOrCreate()

    toSpark(spark)
    
if __name__ == '__main__':
    prc_dt = sys.argv[1]
    job_id = sys.argv[2]
    sys.exit(main())
