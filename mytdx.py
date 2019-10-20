from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams

from datetime import date, datetime, timedelta
import mysql.connector

api = TdxHq_API()

cnx = mysql.connector.connect(user='root',password='lihq8087*)*&',host='127.0.0.1',database='tdx')
cur_list = cnx.cursor(buffered=True)
cur_tran = cnx.cursor(buffered=True)

query_list = ("SELECT code,name,market FROM tdx.list ")

with api.connect('119.147.212.81', 7709):

    add_hq=("INSERT INTO transaction"
        "(id,date,time,price,vol,num,buyorsell) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s)")

    data=[]

    cur_list.execute(query_list)
    for (code,name,market) in cur_list:
        # data=api.get_transaction_data(TDXParams.MARKET_SZ, '000001', 4001,6000)
        # data+=api.get_transaction_data(TDXParams.MARKET_SZ, '000001', 2001,4000)
        data=api.get_transaction_data(market , code ,    0,20)

        count = len(data);

        for i in range(len(data)):
            time=data[i]['time']
            price=data[i]['price']
            vol  =data[i]['vol']
            num  =data[i]['num']
            buyorsell = data[i]['buyorsell']
            data_hq=(code,date(2019,10,18),time,price,vol,num,market)
            # Insert new employee
            cur_tran.execute(add_hq, data_hq)
            cnx.commit()
        cur_tran.close

    cur_list.close()
    cnx.close()
    
    print(count)
    print("Hello world!")
    api.disconnect()
