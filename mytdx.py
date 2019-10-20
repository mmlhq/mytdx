from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams

from datetime import date, datetime, timedelta
import mysql.connector

api = TdxHq_API()

cnx = mysql.connector.connect(user='root',password='lihq8087*)*&',host='127.0.0.1',database='tdx')
cursor = cnx.cursor()

query_list = ("SELECT code,name,market FROM tdx.list ")

with api.connect('119.147.212.81', 7709):

    add_hq=("INSERT INTO transaction"
        "(id,date,time,price,vol,num,buyorsell) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s)")

    data=[]

    cursor.execute(query_list)
    for (code,name,market) in cursor:
        # data=api.get_transaction_data(TDXParams.MARKET_SZ, '000001', 4001,6000)
        # data+=api.get_transaction_data(TDXParams.MARKET_SZ, '000001', 2001,4000)
        data=api.get_transaction_data(TDXParams.MARKET_SZ, '000001',    0,2000)

        count = len(data);

        for i in range(len(data)):
            time=data[i]['time']
            price=data[i]['price']
            vol  =data[i]['vol']
            num  =data[i]['num']
            buyorsell = data[i]['buyorsell']
            data_hq=(1,date(2019,10,18),time,price,vol,num,buyorsell)
            # Insert new employee
            cursor.execute(add_hq, data_hq)
#            cnx.commit()

    cursor.close()
    cnx.close()
    
    print(count)
    print("Hello world!")
    api.disconnect()

