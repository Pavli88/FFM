import argparse
import time
from datetime import date
from datetime import timedelta
from _datetime import datetime
import mysql.connector as sql
import pandas as pd
import numpy as np
import sys
import requests

#Classes

class SQL():

    '''
    SQL query class for reading, writing and updating data to FFM databases.
    '''

    def __init__(self, data_base,user_name,password):

        self.data_base = data_base
        self.user_name = user_name
        self.password = password

        self.db_connection = sql.connect(user=self.user_name, password=self.password, host='localhost',
                                         database=self.data_base)

        self.cursor = self.db_connection.cursor()

    def select_data(self, select_query):

        '''
        Runs select query on database and returns a pandas dataframe
        :param select_query: SQL query
        :return: Pandas dataframe
        '''

        self.select_query = select_query

        self.df = pd.read_sql(self.select_query, con=self.db_connection)

        return self.df

    def insert_data(self, insert_query):

        '''
        Inserts data to the defined database and table
        :param insert_query: Insert query
        '''

        self.insert_query = insert_query

        self.cursor.execute(self.insert_query)

        self.db_connection.commit()

        self.db_connection.close()

class Online_data():

    """
    Integrates intraday quote from IEX Exchange to database for a particular equity security
    """

    def __init__(self,period,ticker):

        self.period = period
        self.ticker = ticker

        if self.period == "intraday":
            self.url = 'https://api.iextrading.com/1.0/stock/' + str(self.ticker) + '/batch?types=chart&range=1d&last=1'
            self.r = requests.get(self.url)
        else:
            self.url = 'https://api.iextrading.com/1.0/stock/' + str(self.ticker) + '/chart/date/' + str(self.period)
            self.r = requests.get(self.url)

    def import_intraday_quote_to_database(self):

        """
        Inserts intraday raw quote data to database
        """

        self.data_frame = pd.read_json(self.r.text)

        self.query = "insert into intraday_quotes (ticker,date,minute,high,low,average,volume,notional,number_of_trades,market_high,market_low,market_average,market_volume,market_notional,market_number_of_trades,open,close,market_open,market_close) values "

        for self.row in range(len(np.asarray(self.data_frame["average"]))):

            self.changed_list = [self.ticker,
                                 self.data_frame.loc[self.row, :]['date'],
                                 self.data_frame.loc[self.row, :]['minute'],
                                 round(float(self.data_frame.loc[self.row, :]['high']),2),
                                 round(float(self.data_frame.loc[self.row, :]['low']),2),
                                 round(float(self.data_frame.loc[self.row, :]['average']),2),
                                 self.data_frame.loc[self.row, :]['volume'],
                                 round(float(self.data_frame.loc[self.row, :]['notional']),2),
                                 self.data_frame.loc[self.row, :]['numberOfTrades'],
                                 round(float(self.data_frame.loc[self.row, :]['marketHigh']),2),
                                 round(float(self.data_frame.loc[self.row, :]['marketLow']),2),
                                 round(float(self.data_frame.loc[self.row, :]['marketAverage']),2),
                                 self.data_frame.loc[self.row, :]['marketVolume'],
                                 round(float(self.data_frame.loc[self.row, :]['marketNotional']),2),
                                 self.data_frame.loc[self.row, :]['marketNumberOfTrades'],
                                 round(float(self.data_frame.loc[self.row, :]['open']),2),
                                 round(float(self.data_frame.loc[self.row, :]['close']),2),
                                 round(float(self.data_frame.loc[self.row, :]['marketOpen']),2),
                                 round(float(self.data_frame.loc[self.row, :]['marketClose']),2)]

            for n  in range(len(self.changed_list)):

                if str(self.changed_list[n]) == 'nan':
                    self.changed_list[n] = 0

            self.string_list = str(self.changed_list)
            self.query_values = "("+self.string_list[1:-1]+")"+","
            self.query += self.query_values

        self.query0 = str(self.query)

        return self.query[:-1]+";"

#FUNCTIONS

#Value area calculator
def value_area_calc(rawdata):

    filename = rawdata
    maxc = np.max(filename[1])

    if maxc < 50:
        locator = 3
    elif maxc < 100 and maxc >= 50:
        locator = 2
    else:
        locator = 1

    sumvol = np.sum(filename[4])

    df = pd.DataFrame({'PRICE':round((filename[1]+filename[2])/2,locator),
                       'VOLUME':filename[4]/sumvol,
                       })

    table = df.groupby('PRICE')['VOLUME'].sum().reset_index(name='VOLUME')
    tablemax = np.max(table['VOLUME'])

    for i,j in zip(np.asarray(table['VOLUME']),range(len(np.asarray(table['VOLUME'])))):
        if i == tablemax:
            maxlocation = j

    val = maxlocation
    vah = maxlocation
    valnum = 1
    vahnum = 1
    valswitch = 1
    vahswitch = 1

    while tablemax <= 0.7:
        try:
            val = table.loc[maxlocation-valnum]

        except:
            valswitch = 2
        try:

            vah = table.loc[maxlocation+vahnum]

        except:
            vahswitch =2

        if vah['VOLUME'] > val['VOLUME']:

            if vahswitch == 1:
                tablemax = tablemax+vah["VOLUME"]
                vahnum = vahnum+1

            else:
                tablemax = tablemax + val["VOLUME"]
                valnum = valnum + 1
        else:

            if valswitch == 1:
                tablemax = tablemax+val["VOLUME"]
                valnum = valnum + 1
            else:
                tablemax = tablemax + vah["VOLUME"]
                vahnum = vahnum + 1

    return val['PRICE'],vah['PRICE'],table['PRICE'],table['VOLUME'],locator

#ARGUMENTS

parser = argparse.ArgumentParser()
parser.add_argument("--rundate", help="Specifies on which date to run production. Switch: specific")
parser.add_argument("--ticker", help="Defining the list of ticker or single ticker to produce data. Switches: ALL - All tickers; Specific ticker code",)
parser.add_argument("--download", help="Download latest intraday data. Switch: Yes")
parser.add_argument("--dailycalc", help="Daily data production. Switch: Yes")
parser.add_argument("--updatedaily", help="Updates the latest daily data row. Switch: Yes")
parser.add_argument("--unitcalc", help="Calculates unit and market level data. Switch: Yes")
parser.add_argument("--confcalc", help="Calculates confidence data. Switch: Yes")
parser.add_argument("--patterncalc", help="Calculates pattern data. Switch: Yes")
parser.add_argument("--updatepattern", help="Updates latest pattern data. Switch: Yes")
parser.add_argument("--chartmail", help="Calculates chart mails. Switch: Yes")
parser.add_argument("--stpattern", help="Checks securities with strong pattern. Switch: Yes")
parser.add_argument("--forcerun", help="Forces script to run if market is open. Switch: Yes")
parser.add_argument("--tradereturn", help="Calculates trade return on positions. Switch: Yes")
parser.add_argument("--portret", help="Calculates portfolio returns. Switch: Yes")
parser.add_argument("--backup", help="Creates database backup file. Switch: Yes")
parser.add_argument("--datechecker", help="Checks latest day on daily data. Switch: Yes")
parser.add_argument("--env", help="Environment switch. Default:prod; Switches: test; dev ")
parser.add_argument("--force_save_drel", help="Forces to resave daily_rel data. Switch: Yes")
parser.add_argument("--db_user_name", help="User name for database login. Switch: username")
parser.add_argument("--db_password", help="Password for database login. Switch: password")
args = parser.parse_args()

# GLOBAL VARIABLES

# Date variables
date0 = time.strftime("%Y:%m:%d")
day = time.strftime("%A")
begtime = time.strftime("%H:%M:%S")
today = date.today()

#Defining production date
if args.rundate != None:

    portdate1 = args.rundate
    portdate = datetime.date(datetime(year=int(portdate1[0:4]),month=int(portdate1[4:6]),day=int(portdate1[6:8])))
    weekday = portdate.weekday()

else:
    if today.weekday() == 0:

        if begtime > "22:18:00":

            portdate = today

        else:
            delta = timedelta(days=-3)
            portdate = today + delta

    elif today.weekday() == 6:

        delta = timedelta(days=-2)
        portdate = today + delta

    elif today.weekday() == 5:

        delta = timedelta(days=-1)
        portdate = today + delta

    else:
        if begtime < "15:00:00":

            delta = timedelta(days=-1)
            portdate = today + delta

        else:
            portdate = today

#SQL query date
query_date0 = str(portdate)
query_date = query_date0[0:4]+query_date0[5:7]+query_date0[8:10]

#Environment definition
if args.env == "test":

    data_base = "test"

elif args.env == "dev":

    data_base = "dev"

else:

    data_base = "prod"

print('''********************************************
           DAILY DATA PRODUCTION
********************************************''')
print("-----------------")
print("| DAY PARAMTERS |")
print("-----------------")
print("Date: " + date0)
print("Day: " + day)
print("Start time: " + begtime)
print("Portfolio date: " + str(portdate))
print("Environment: "+str(data_base))
print("")
print("------------------")
print("| INITIALIZATION |")
print("------------------")

#Defining tickers to run production on
if args.ticker == "ALL":

    ticker_list = SQL(data_base,args.db_user_name,args.db_password).select_data("select ticker from eq_info")
    file2 = list(ticker_list['ticker'])
    print("[" + time.strftime("%H:%M:%S") + "]" + "DATA PRODUCTION FOR ALL EQUITY TICKER")

else:
    file2 = [str(args.ticker)]
    print("[" + time.strftime("%H:%M:%S") + "]" + "DATA PRODUCTION FOR "+str(file2))

print("[" + time.strftime("%H:%M:%S") + "]" + "ANALYZING IF PRODUCTION CAN BE KICKED OFF...")

#Checks if the rundate is weekend. If yes it shuts down data production
if args.rundate != None:

    if (weekday == 5 or weekday == 6):

        print("This is a weekend production is killed.")
        sys.exit()

#Switch to force run data production while market is open
if args.forcerun == "Yes":

    print("[" + time.strftime("%H:%M:%S") + "]" + "PRODUCTION IS FORCED TO RUN")
    begtime = "14:00:00"

else:

    pass

#Checking if the market is open or not
if begtime > "15:00:00" and begtime < "22:15:00":

    print("[" + time.strftime("%H:%M:%S") + "]" + "MARKET IS OPEN PRODUCTION STOPPED !")
    sys.exit()

else:
    print("")
    print("----------------------------")
    print("| START OF DATA PRODUCTION |")
    print("----------------------------")

    #INTRADAY DATA DOWNLOAD SECTION
    if args.download == "Yes":

        print("[" + time.strftime("%H:%M:%S") + "]" + "DOWNLOADING INTRA DAY QUOTES FROM INTERNET")

        for sec in file2:

            #Checking if data was already downloaded
            data_check = SQL(data_base,args.db_user_name,args.db_password).select_data("select*from process_monitor where ticker = '"+str(sec)+"' and date = '"+str(query_date)+"'")

            if data_check['intraday_data_download'].values == "Yes":

                print("[" + time.strftime("%H:%M:%S") + "]" + "DATA EXISTS IN DATA BASE FOR -> " + str(sec) + " DATE: " + str(query_date))
                pass

            else:
                #Defining if we download data for the last day or for a specific day

                '''
                DOWNLOAD OPTION CAN DOWNLOAD QUOTE DATA FOR T-1 IN DEFAULT MODE AND FOR A DEFINED DATE
                '''

                try:
                    #Specific date
                    if args.rundate != None:
                        intdata = Online_data(ticker=sec, period=str(args.rundate)).import_intraday_quote_to_database()
                        SQL(data_base,args.db_user_name,args.db_password).insert_data(intdata)
                        SQL(data_base,args.db_user_name,args.db_password).insert_data("insert into process_monitor (ticker,intraday_data_download,date) values ('"+str(sec)+"','Yes','"+str(args.rundate)+"')")

                        print("[" + time.strftime("%H:%M:%S") + "]" + "DATA DOWNLOAD AND SAVE TO DATABASE -> " + str(sec) + " SUCCESS!   " + str(args.rundate))

                    # Production date
                    else:
                        intdata = Online_data(ticker=sec, period=query_date).import_intraday_quote_to_database()
                        SQL(data_base,args.db_user_name,args.db_password).insert_data(intdata)
                        SQL(data_base,args.db_user_name,args.db_password).insert_data("insert into process_monitor (ticker,intraday_data_download,date) values ('" + str(sec) + "','Yes','" + str(query_date) + "')")

                        print("[" + time.strftime("%H:%M:%S") + "]" + "DOWNLOADING DATA AND SAVING TO DATABASE -> " + str(sec) + " SUCCESS! " + str(query_date))
                except:
                    print("[" + time.strftime("%H:%M:%S") + "]" + "DOWNLOADING DATA AND SAVING TO DATABASE -> " + str(sec) + " FAILED!")

    #DAILY DATA CALCULATION FROM INTRADAY QUOTES
    if args.dailycalc == "Yes":

        print("")
        print("---------------------")
        print("| RUNNING DAILYCALC |")
        print("---------------------")

        # Situation Table
        sit1 = pd.Series(
            [156, 136, 146, 158, 138, 148, 1510, 1310, 1410, 157, 137, 147, 159, 139, 149, 257, 237, 247, 259, 239, 249,
             2510,2310, 2410, 256, 236, 246, 258, 238, 248]
        )

        sit2 = pd.Series(
            ['Very-Str', 'Slowing', 'Str-Cont', 'Mod-Str', 'Slw-Blncing', 'Mod-Str-Blncing', 'Blncing','Blncing-Wkning',
             'Blncing', 'Uncl', 'Wk', 'Wk-Blncing', 'Wkning', 'Mod-wk', 'Wkning-Blncing', 'Very-Wk', 'Slowing','Wk-Cont',
             'Mod-Wk', 'Slo-Blncing', 'Mod-Wk-Blncing', 'Blncing', 'Blncing-Strning', 'Blncing', 'Uncl', 'Str','Str-Blncing',
             'Strning', 'Mod-Str', 'Strning']
        )

        sit = pd.DataFrame({'A': sit1,
                            'B': sit2,
                            })

        # ---------------------------------------
        # BEGINNING OF OPERATIONS
        # ---------------------------------------

        for security in file2:

            # ---------------------------------------
            # START OF GENERAL DATA POINT CALCULATIONS
            # ---------------------------------------

            '''
            DAILYCALC FIRTS CALCULATES GENERAL DATAPOINTS AND IT SAVES TO THE DATABASE
            '''

            try:
                # Checking if relative data was already calculated
                data_check = SQL(data_base,args.db_user_name,args.db_password).select_data("select*from process_monitor where ticker = '" + str(security) + "' and date = '" + str(query_date) + "'")

                print("")
                print('***' + str(security) + '***')
                print("")

                # Fetching intraday quotes from database

                '''
                DAILY CALC CAN FETCH DATA FOR A SPECIFIC DATE AND IN DEFAULT MODE FOR T-1 DATE.
                '''

                # Fetching for specific date
                if args.rundate != None:

                    import_quotes = SQL(data_base,args.db_user_name,args.db_password).select_data("select market_close,market_high,market_low,market_open,market_volume from intraday_quotes where ticker = '" + str(security) + "' and date = '" + str(args.rundate) + "'")
                    import_quotes.columns = [0, 1, 2, 3, 4]
                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Fetching quotes from databas -> Date: ' + str(
                        args.rundate))

                # Fetching for T-1
                else:

                    import_quotes = SQL(data_base,args.db_user_name,args.db_password).select_data("select market_close,market_high,market_low,market_open,market_volume from intraday_quotes where ticker = '" + str(security) + "' and date = '" + str(query_date) + "'")
                    import_quotes.columns = [0, 1, 2, 3, 4]
                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Fetching quotes from databas -> Date: ' + str(
                        query_date))

                s = 2

                if s == 1:
                    re = -0.1
                    tiz = 10
                elif s == 2:
                    re = -0.01
                    tiz = 100
                else:
                    re = -0.001
                    tiz = 1000


                # Cecking missing data and correcting it
                filename = import_quotes
                closecol = np.asarray(filename[0])
                opencol = np.asarray(filename[3])

                closecol2 = []

                for i, j in zip(closecol, opencol):

                    if i > 0:
                        closecol2.append(i)
                    else:
                        closecol2.append(j)

                filename[0] = closecol2
                filename = filename.dropna(how='any')
                newindex = np.arange(0, len(np.asarray(filename[0])), 1)
                filename = filename.set_index(newindex)
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Formating dataframe -> Completed')

                ppmax = (np.asanyarray(filename[0]).size) - 1
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Total Number of Rows:', '', ppmax)

                novol = filename.loc[:, 0:3]
                df = filename.loc[:, 2:3]
                meanall = df.mean(1)
                round2 = meanall.round(s)


                # Intraday total volume
                vol = filename.loc[:, 4]
                sumvol = vol.values.sum()
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Intraday Volume: ' + str(sumvol))


                # Intraday max and min
                allmax = round(novol.values.max(), s)
                allmin = round(novol.values.min(), s)

                print("[" + time.strftime("%H:%M:%S") + "] " + 'Intraday Max: ' + str(allmax))
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Intraday Min: ' + str(allmin))

                if ppmax > 30:
                    df2 = filename.loc[0:30, 0:3]
                    rmax1 = filename.loc[0:30, 1].values.max()
                    rmin1 = filename.loc[0:30, 2].values.min()
                    print('Dataframe1 ', 'Max:', rmax1, '', 'Min: ', rmin1)
                    if ppmax > 60:
                        df3 = filename.loc[31:60, 0:3]
                        rmax2 = filename.loc[31:60, 1].values.max()
                        rmin2 = filename.loc[31:60, 2].values.min()
                        print('Dataframe2 ', 'Max:', rmax2, '', 'Min: ', rmin2)
                        if ppmax > 90:
                            df4 = filename.loc[61:90, 0:3]
                            rmax3 = filename.loc[61:90, 1].values.max()
                            rmin3 = filename.loc[61:90, 2].values.min()
                            print('Dataframe3 ', 'Max:', rmax3, '', 'Min: ', rmin3)
                            if ppmax > 120:
                                df5 = filename.loc[91:120, 0:3]
                                rmax4 = filename.loc[91:120, 1].values.max()
                                rmin4 = filename.loc[91:120, 2].values.min()
                                print('Dataframe4 ', 'Max:', rmax4, '', 'Min: ', rmin4)
                                if ppmax > 150:
                                    df6 = filename.loc[121:150, 0:3]
                                    rmax5 = filename.loc[121:150, 1].values.max()
                                    rmin5 = filename.loc[121:150, 2].values.min()
                                    print('Dataframe5 ', 'Max:', rmax5, '', 'Min: ', rmin5)

                                    if ppmax > 180:
                                        df7 = filename.loc[151:180, 0:3]
                                        rmax6 = filename.loc[151:180, 1].values.max()
                                        rmin6 = filename.loc[151:180, 2].values.min()
                                        print('Dataframe6 ', 'Max:', rmax6, '', 'Min: ', rmin6)

                                        if ppmax > 210:
                                            df8 = filename.loc[181:210, 0:3]
                                            rmax7 = filename.loc[181:210, 1].values.max()
                                            rmin7 = filename.loc[181:210, 2].values.min()
                                            print('Dataframe7 ', 'Max:', rmax7, '', 'Min: ', rmin7)

                                            if ppmax > 240:
                                                df9 = filename.loc[211:240, 0:3]
                                                rmax8 = filename.loc[211:240, 1].values.max()
                                                rmin8 = filename.loc[211:240, 2].values.min()
                                                print('Dataframe8 ', 'Max:', rmax8, '', 'Min: ', rmin8)

                                                if ppmax > 270:
                                                    df10 = filename.loc[241:270, 0:3]
                                                    rmax9 = filename.loc[241:270, 1].values.max()
                                                    rmin9 = filename.loc[241:270, 2].values.min()
                                                    print('Dataframe9 ', 'Max:', rmax9, '', 'Min: ', rmin9)

                                                    if ppmax > 300:
                                                        df11 = filename.loc[271:300, 0:3]
                                                        rmax10 = filename.loc[271:300, 1].values.max()
                                                        rmin10 = filename.loc[271:300, 2].values.min()
                                                        print('Dataframe10 ', 'Max:', rmax10, '', 'Min: ', rmin10)

                                                        if ppmax > 330:
                                                            df12 = filename.loc[301:330, 0:3]
                                                            rmax11 = filename.loc[301:330, 1].values.max()
                                                            rmin11 = filename.loc[301:330, 2].values.min()
                                                            print('Dataframe11 ', 'Max:', rmax11, '', 'Min: ', rmin11)

                                                            if ppmax > 360:
                                                                df13 = filename.loc[331:360, 0:3]
                                                                rmax12 = filename.loc[331:360, 1].values.max()
                                                                rmin12 = filename.loc[331:360, 2].values.min()
                                                                print('Dataframe12 ', 'Max:', rmax12, '', 'Min: ',
                                                                      rmin12)

                                                                if ppmax >= 361:
                                                                    df14 = filename.loc[361:ppmax, 0:3]
                                                                    closeprice = df14.loc[ppmax, 3]
                                                                    rmax13 = filename.loc[361:ppmax, 1].values.max()
                                                                    rmin13 = filename.loc[361:ppmax, 2].values.min()
                                                                    print('Dataframe13 ', 'Max:', rmax13, '', 'Min: ',
                                                                          rmin13)

                                                            elif ppmax <= 360:
                                                                df13 = filename.loc[331:ppmax, 0:3]
                                                                closeprice = df13.loc[ppmax, 3]
                                                                rmax12 = filename.loc[331:ppmax, 1].values.max()
                                                                rmin12 = filename.loc[331:ppmax, 2].values.min()
                                                                rmax13 = 0
                                                                rmin13 = 0
                                                                print('Dataframe12 ', 'Max:', rmax12, '', 'Min: ',
                                                                      rmin12)

                                                        elif ppmax <= 330:
                                                            df12 = filename.loc[301:ppmax, 0:3]
                                                            closeprice = df12.loc[ppmax, 3]
                                                            rmax11 = filename.loc[301:ppmax, 1].values.max()
                                                            rmin11 = filename.loc[301:ppmax, 2].values.min()
                                                            rmax12 = 0
                                                            rmin12 = 0
                                                            rmax13 = 0
                                                            rmin13 = 0
                                                            print('Dataframe11 ', 'Max:', rmax11, '', 'Min: ', rmin11)


                                                    elif ppmax <= 300:
                                                        df11 = filename.loc[271:ppmax, 0:3]
                                                        closeprice = df11.loc[ppmax, 3]
                                                        rmax10 = filename.loc[271:ppmax, 1].values.max()
                                                        rmin10 = filename.loc[271:ppmax, 2].values.min()
                                                        rmax11 = 0
                                                        rmin11 = 0
                                                        rmax12 = 0
                                                        rmin12 = 0
                                                        rmax13 = 0
                                                        rmin13 = 0
                                                        print('Dataframe10 ', 'Max:', rmax10, '', 'Min: ', rmin10)

                                                elif ppmax <= 270:
                                                    df10 = filename.loc[241:ppmax, 0:3]
                                                    closeprice = df10.loc[ppmax, 3]
                                                    rmax9 = filename.loc[241:ppmax, 1].values.max()
                                                    rmin9 = filename.loc[241:ppmax, 2].values.min()
                                                    rmax10 = 0
                                                    rmin10 = 0
                                                    rmax11 = 0
                                                    rmin11 = 0
                                                    rmax12 = 0
                                                    rmin12 = 0
                                                    rmax13 = 0
                                                    rmin13 = 0
                                                    print('Dataframe9 ', 'Max:', rmax9, '', 'Min: ', rmin9)

                                            elif ppmax <= 240:
                                                df9 = filename.loc[211:ppmax, 0:3]
                                                closeprice = df9.loc[ppmax, 3]
                                                rmax8 = filename.loc[211:ppmax, 1].values.max()
                                                rmin8 = filename.loc[211:ppmax, 2].values.min()
                                                rmax9 = 0
                                                rmin9 = 0
                                                rmax10 = 0
                                                rmin10 = 0
                                                rmax11 = 0
                                                rmin11 = 0
                                                rmax12 = 0
                                                rmin12 = 0
                                                rmax13 = 0
                                                rmin13 = 0
                                                print('Dataframe8 ', 'Max:', rmax8, '', 'Min: ', rmin8)

                                        elif ppmax <= 210:
                                            df8 = filename.loc[181:ppmax, 0:3]
                                            closeprice = df8.loc[ppmax, 3]
                                            rmax7 = filename.loc[181:ppmax, 1].values.max()
                                            rmin7 = filename.loc[181:ppmax, 2].values.min()
                                            rmax8 = 0
                                            rmin8 = 0
                                            rmax9 = 0
                                            rmin9 = 0
                                            rmax10 = 0
                                            rmin10 = 0
                                            rmax11 = 0
                                            rmin11 = 0
                                            rmax12 = 0
                                            rmin12 = 0
                                            rmax13 = 0
                                            rmin13 = 0
                                            print('Dataframe7 ', 'Max:', rmax7, '', 'Min: ', rmin7)

                                    elif ppmax <= 180:
                                        df7 = filename.loc[151:ppmax, 0:3]
                                        closeprice = df7.loc[ppmax, 3]
                                        rmax6 = filename.loc[151:ppmax, 1].values.max()
                                        rmin6 = filename.loc[151:ppmax, 2].values.min()
                                        rmax7 = 0
                                        rmin7 = 0
                                        rmax8 = 0
                                        rmin8 = 0
                                        rmax9 = 0
                                        rmin9 = 0
                                        rmax10 = 0
                                        rmin10 = 0
                                        rmax11 = 0
                                        rmin11 = 0
                                        rmax12 = 0
                                        rmin12 = 0
                                        rmax13 = 0
                                        rmin13 = 0
                                        print('Dataframe6 ', 'Max:', rmax6, '', 'Min: ', rmin6)

                                elif ppmax <= 150:
                                    df6 = filename.loc[121:ppmax, 0:3]
                                    closeprice = df6.loc[ppmax, 3]
                                    rmax5 = filename.loc[121:ppmax, 1].values.max()
                                    rmin5 = filename.loc[121:ppmax, 2].values.min()
                                    rmax6 = 0
                                    rmin6 = 0
                                    rmax7 = 0
                                    rmin7 = 0
                                    rmax8 = 0
                                    rmin8 = 0
                                    rmax9 = 0
                                    rmin9 = 0
                                    rmax10 = 0
                                    rmin10 = 0
                                    rmax11 = 0
                                    rmin11 = 0
                                    rmax12 = 0
                                    rmin12 = 0
                                    rmax13 = 0
                                    rmin13 = 0
                                    print('Dataframe5 ', 'Max:', rmax5, '', 'Min: ', rmin5)

                            elif ppmax <= 120:
                                df5 = filename.loc[91:ppmax, 0:3]
                                closeprice = df5.loc[ppmax, 3]
                                rmax4 = filename.loc[91:ppmax, 1].values.max()
                                rmin4 = filename.loc[91:ppmax, 2].values.min()
                                print('Dataframe4 ', 'Max:', rmax4, '', 'Min: ', rmin4)

                        elif ppmax <= 90:
                            df4 = filename.loc[61:ppmax, 0:3]
                            closeprice = df4.loc[ppmax, 3]
                            rmax3 = filename.loc[61:ppmax, 1].values.max()
                            rmin3 = filename.loc[61:ppmax, 2].values.min()
                            print('Dataframe3 ', 'Max:', rmax3, '', 'Min: ', rmin3)

                    elif ppmax <= 60:
                        df3 = filename.loc[31:ppmax, 0:3]
                        closeprice = df3.loc[ppmax, 3]
                        rmax2 = filename.loc[31:ppmax, 1].values.max()
                        rmin2 = filename.loc[31:ppmax, 2].values.min()
                        print('Dataframe2 ', 'Max:', rmax2, '', 'Min: ', rmin2)

                elif ppmax <= 30:
                    df2 = filename.loc[0:ppmax, 0:3]
                    closeprice = df2.loc[ppmax, 3]
                    rmax1 = filename.loc[0:ppmax, 1].values.max()
                    rmin1 = filename.loc[0:ppmax, 2].values.min()
                    print('Dataframe1 ', 'Max:', rmax1, '', 'Min: ', rmin1)

                openprice = df2.loc[0, 0]
                max1 = df2.values.max()

                print("[" + time.strftime("%H:%M:%S") + "] " + 'Open price: ', openprice)
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Close price: ', closeprice)


                # Initial balance min
                ini = filename.loc[0:60, 0:3]
                initalmin = ini.values.min()
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Initial Balance Min: ', initalmin)


                # Initial balance max
                ini2 = filename.loc[0:60, 0:3]
                initalmax = ini2.values.max()
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Initial Balance Max: ', initalmax)


                # Selling Range Extension
                if allmin < initalmin:
                    sre = -1
                else:
                    sre = 0
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Selling Range Extension: ', sre)


                # Buying Range Extension
                if allmax > initalmax:
                    bre = 1
                else:
                    bre = 0
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Buying Range Extension: ', bre)


                # Range Extension Succes
                if closeprice < initalmin:
                    res = -1
                elif initalmin < closeprice < initalmax:
                    res = 0
                else:
                    res = 1
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Range Extension Succes: ', res)

                maxar = pd.Series(
                    [rmax1, rmax2, rmax3, rmax4, rmax5, rmax6, rmax7, rmax8, rmax9, rmax10, rmax11, rmax12, rmax13])
                minar = pd.Series(
                    [rmin1, rmin2, rmin3, rmin4, rmin5, rmin6, rmin7, rmin8, rmin9, rmin10, rmin11, rmin12, rmin13])
                ardiff = (maxar - minar) * tiz
                allmaxar = pd.Series(
                    [allmax, allmax, allmax, allmax, allmax, allmax, allmax, allmax, allmax, allmax, allmax, allmax,
                     allmax])
                allminar = pd.Series(
                    [allmin, allmin, allmin, allmin, allmin, allmin, allmin, allmin, allmin, allmin, allmin, allmin,
                     allmin, ])
                mindiff = (minar - allmin) * tiz
                maxdiff = (allmaxar - maxar) * tiz
                totdiff = ardiff + mindiff + maxdiff


                # Rotation factor
                def rfactormax(rmaxi, rmaxj):
                    if rmaxi > rmaxj:
                        return 1
                    elif rmaxi == rmaxj:
                        return 0
                    else:
                        return -1

                rf1 = rfactormax(rmax2, rmax1)
                rf2 = rfactormax(rmax3, rmax2)
                rf3 = rfactormax(rmax4, rmax3)
                rf4 = rfactormax(rmax5, rmax4)
                rf5 = rfactormax(rmax6, rmax5)
                rf6 = rfactormax(rmax7, rmax6)
                rf7 = rfactormax(rmax8, rmax7)
                rf8 = rfactormax(rmax9, rmax8)
                rf9 = rfactormax(rmax10, rmax9)
                rf10 = rfactormax(rmax11, rmax10)
                rf11 = rfactormax(rmax12, rmax11)
                rf12 = rfactormax(rmax13, rmax12)

                rmatot = rf1 + rf2 + rf3 + rf4 + rf5 + rf6 + rf7 + rf8 + rf9 + rf10 + rf11 + rf12

                def rfactormin(rmini, rminj):
                    if rmini > rminj:
                        return 1
                    elif rmini == rminj:
                        return 0
                    else:
                        return -1

                rfm1 = rfactormin(rmin2, rmin1)
                rfm2 = rfactormin(rmin3, rmin2)
                rfm3 = rfactormin(rmin4, rmin3)
                rfm4 = rfactormin(rmin5, rmin4)
                rfm5 = rfactormin(rmin6, rmin5)
                rfm6 = rfactormin(rmin7, rmin6)
                rfm7 = rfactormin(rmin8, rmin7)
                rfm8 = rfactormin(rmin9, rmin8)
                rfm9 = rfactormin(rmin10, rmin9)
                rfm10 = rfactormin(rmin11, rmin10)
                rfm11 = rfactormin(rmin12, rmin11)
                rfm12 = rfactormin(rmin13, rmin12)

                rmitot = rfm1 + rfm2 + rfm3 + rfm4 + rfm5 + rfm6 + rfm7 + rfm8 + rfm9 + rfm10 + rfm11 + rfm12

                rf = rmatot + rmitot
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Roration Factor: ', rf)


                # Market Profile----------------------------------------------------------------------------------

                pricerange = pd.Series(np.arange(allmax, allmin, re))

                pricelen = len(pricerange)
                maxmindiff = int((allmax - allmin) * tiz)

                if pricelen > maxmindiff:
                    pricesav = prrr = pricerange.head(maxmindiff)
                else:
                    pricesav = pricerange

                dfpricerange = pd.DataFrame({'A': pricesav,
                                             })
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Price Range Lenght: ', pricelen)

                def period(rmaxi, x):
                    if rmaxi == 0:
                        return np.full(pricelen, 0)
                    else:
                        max1 = int(round(maxdiff[x]))
                        mid1 = int(round(ardiff[x]))
                        min1 = int(round(mindiff[x]))

                        return np.append([np.full(max1, 0)], [np.append([np.full(mid1, 1)], [np.full(min1, 0)])])

                wh1 = period(rmax1, 0)
                wh2 = period(rmax2, 1)
                wh3 = period(rmax3, 2)
                wh4 = period(rmax4, 3)
                wh5 = period(rmax5, 4)
                wh6 = period(rmax6, 5)
                wh7 = period(rmax7, 6)
                wh8 = period(rmax8, 7)
                wh9 = period(rmax9, 8)
                wh10 = period(rmax10, 9)
                wh11 = period(rmax11, 10)
                wh12 = period(rmax12, 11)
                wh13 = period(rmax13, 12)

                if len(wh5) != len(wh4):
                    wh5 = np.full(len(wh4), 0)

                if len(wh6) != len(wh5):
                    wh6 = np.full(len(wh5), 0)

                if len(wh7) != len(wh6):
                    wh7 = np.full(len(wh6), 0)

                if len(wh8) != len(wh7):
                    wh8 = np.full(len(wh7), 0)

                if len(wh9) != len(wh8):
                    wh9 = np.full(len(wh8), 0)

                if len(wh10) != len(wh9):
                    wh10 = np.full(len(wh9), 0)

                if len(wh11) != len(wh10):
                    wh11 = np.full(len(wh10), 0)

                if len(wh12) != len(wh11):
                    wh12 = np.full(len(wh11), 0)

                if len(wh13) != len(wh12):
                    wh13 = np.full(len(wh12), 0)


                # Rounded price + volume table, volume at price table
                dfvol = pd.DataFrame({'A': round2,
                                      'B': vol, })

                dfvolsum = dfvol.groupby('A').sum()


                # Period table
                perioddf = pd.DataFrame({'A': wh1,
                                         'B': wh2,
                                         'C': wh3,
                                         'D': wh4,
                                         'E': wh5,
                                         'F': wh6,
                                         'G': wh7,
                                         'H': wh8,
                                         'I': wh9,
                                         'J': wh10,
                                         'K': wh11,
                                         'L': wh12,
                                         'M': wh13,
                                         })

                summarketdf = perioddf.sum(1)


                # Market Profile cordinates
                mp = pd.DataFrame({'A': pricesav,
                                   'B': summarketdf, })


                # Point of controll
                pocmaxpos = mp['B'].values.max()
                pocmax = mp[mp.B == pocmaxpos]
                poc = round(pocmax['A'].mean(), s)

                print("[" + time.strftime("%H:%M:%S") + "] " + 'Point of Controll: ', poc)


                # VALUE AREA
                try:

                    valuearea = value_area_calc(filename)
                    vah = valuearea[1]
                    val = valuearea[0]

                except:

                    max1 = mp['B'].values.max()

                    pocva = mp[mp.B == max1]

                    poc1 = pocva['A'].values.max()

                    pocu = mp[mp.A > poc1]
                    pocd = mp[mp.A < poc1]

                    val70 = mp['B'].values.sum() * 0.7

                    bot = int(pocu['B'].values.size)
                    bot1 = int(pocu['B'].tail(1))
                    abo = int(pocd['B'].head(1))

                    tpoup = pocu['B'].values.sum()
                    tpodo = pocd['B'].values.sum()

                    pocsize = int(pocva['B'].values.size)

                    if (tpoup > tpodo and pocsize > 1):
                        pocva = pocva.head(1)
                    elif (tpoup < tpodo and pocsize > 1):
                        pocva = pocva.tail(1)

                    osz = int(pocva['B'].mean())

                    nk = 1
                    nm = 1
                    kk = 2
                    dk = 2
                    vah = pocu['A'].tail(nk).max()
                    val = pocd['A'].head(nm).min()

                    while osz < val70:
                        if (bot1 == abo and tpoup > tpodo):
                            osz = osz + bot1
                            nk = nk + 1
                            vah = pocu['A'].tail(nk).max()
                            bot1 = pocu.loc[bot - kk, 'B']
                            kk = kk + 1
                        elif (bot1 == abo and tpoup < tpodo):
                            osz = osz + abo
                            nm = nm + 1
                            val = pocd['A'].head(nm).min()
                            bot1 = pocd.loc[bot + kk, 'B']
                            kk = kk + 1
                        elif bot1 > abo:
                            osz = osz + bot1
                            nk = nk + 1
                            vah = pocu['A'].tail(nk).max()
                            bot1 = pocu.loc[bot - kk, 'B']
                            kk = kk + 1

                        else:
                            osz = osz + abo
                            nm = nm + 1
                            val = pocd['A'].head(nm).min()
                            abo = pocd.loc[bot + dk, 'B']
                            kk = kk + 1

                print("[" + time.strftime("%H:%M:%S") + "] " + 'Value Area High: ', vah)
                print("[" + time.strftime("%H:%M:%S") + "] " + 'Value Area Low: ', val)


                # TAIL
                max1 = mp['B'].values.max()
                pocva = mp[mp.B == max1]
                poc1 = pocva['A'].values.max()
                pocu = mp[mp.A > poc1]
                pocd = mp[mp.A < poc1]
                stail = pocu[pocu.B == 1].size
                btail = pocd[pocd.B == 1].size

                if stail > btail:
                    if closeprice > vah:
                        tail = 1
                    else:
                        tail = -1
                elif stail < btail:
                    if closeprice < val:
                        tail = -1
                    else:
                        tail = 1
                else:
                    tail = 0

                print("[" + time.strftime("%H:%M:%S") + "] " + 'Tail: ', tail)


                # Netural Day

                if (sre == -1 and bre == 1):
                    natd = poc
                else:
                    natd = 0

                print("[" + time.strftime("%H:%M:%S") + "] " + 'Netural Day: ', natd)


                # Volume calculations

                # The volume of the 30 min ranges
                vol1 = filename.loc[0:30, 4].values.sum()
                vol2 = filename.loc[31:60, 4].values.sum()
                vol3 = filename.loc[61:90, 4].values.sum()
                vol4 = filename.loc[91:120, 4].values.sum()
                vol5 = filename.loc[121:150, 4].values.sum()
                vol6 = filename.loc[151:180, 4].values.sum()
                vol7 = filename.loc[181:210, 4].values.sum()
                vol8 = filename.loc[211:240, 4].values.sum()
                vol9 = filename.loc[241:270, 4].values.sum()
                vol10 = filename.loc[271:300, 4].values.sum()
                vol11 = filename.loc[301:330, 4].values.sum()
                vol12 = filename.loc[331:360, 4].values.sum()
                vol13 = filename.loc[361:ppmax, 4].values.sum()


                # % of the 30 min ranges relateive to the total volume
                volper1 = vol1 / sumvol
                volper2 = vol2 / sumvol
                volper3 = vol3 / sumvol
                volper4 = vol4 / sumvol
                volper5 = vol5 / sumvol
                volper6 = vol6 / sumvol
                volper7 = vol7 / sumvol
                volper8 = vol8 / sumvol
                volper9 = vol9 / sumvol
                volper10 = vol10 / sumvol
                volper11 = vol11 / sumvol
                volper12 = vol12 / sumvol
                volper13 = vol13 / sumvol

                if data_check["daily_gen"].values == "Yes":

                    print("[" + time.strftime("%H:%M:%S") + "]" + " DAILY_GEN DATA EXISTS IN DATA BASE FOR -> " + str(security) + " DATE: " + str(query_date))

                    pass

                else:

                    # Writing general daily data to database
                    print("[" + time.strftime("%H:%M:%S") + "] " + "WRITING DAILY_GEN DATA TO DATABASE")
                    SQL(data_base,args.db_user_name,args.db_password).insert_data(
                        "insert into daily_gen (date,ticker,open,min,max,close,inmin,inmax,bre,sre,res,rf,vol,poc,vah,val,ntfd,tpomax) "
                        "values ('" + str(portdate) +
                        "','" + str(security) +
                        "','" + str(openprice) +
                        "','" + str(allmin) +
                        "','" + str(allmax) +
                        "','" + str(closeprice) +
                        "','" + str(initalmin) +
                        "','" + str(initalmax) +
                        "','" + str(bre) +
                        "','" + str(sre) +
                        "','" + str(res) +
                        "','" + str(rf) +
                        "','" + str(sumvol) +
                        "','" + str(poc) +
                        "','" + str(vah) +
                        "','" + str(val) +
                        "','" + str(natd) +
                        "','" + str(max1) + "')")

                    SQL(data_base,args.db_user_name,args.db_password).insert_data("update process_monitor set daily_gen = 'Yes' where ticker = '" + str(security) + "' and date = '" + str(query_date) + "'")

                #-----------------------------------------------
                # START OF RELATIVE DATA POINT CALCULATIONS
                # -----------------------------------------------

                print("[" + time.strftime("%H:%M:%S") + "] " + "DAILY_REL CALCULATION")

                '''
                DEFAULT VALUE ALWAYS POINTS TO THE LATEST TWO DATES. SPECIFIC DATE POINTS TO THE SPECIFIC DATE AND SPECIFIC DATE-1 DATE.
                '''

                if args.rundate != None:

                    #Specific date
                    filename = SQL(data_base,args.db_user_name,args.db_password).select_data("select * from (select * from dev.daily_gen where ticker = '" + str(security) + "' and date <= '"+str(args.rundate)+"' order by date desc limit 2) sub order by date asc")

                else:

                    #Default value
                    filename = SQL(data_base,args.db_user_name,args.db_password).select_data("select * from (select * from dev.daily_gen where ticker = '"+str(security)+"' order by date desc limit 2) sub order by date asc")

                print(filename)
                # Initiative Selling activity
                try:

                    pk = 1
                    pm = 0

                    if (filename.loc[pk, 'min'] < filename.loc[pm, 'val'] and filename.loc[pk, 'close'] < filename.loc[pm, 'val']):
                        ins = -1
                    else:
                        ins = 0

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initiative selling: "+str(ins))

                except:
                    ins = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initiative selling calculation is not possible")


                # Initiative buying activity
                try:
                    if (filename.loc[pk, 'max'] > filename.loc[pm, 'vah'] and filename.loc[pk, 'close'] > filename.loc[pm, 'vah']):
                        inb = 1
                    else:
                        inb = 0

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initiative buying: " + str(inb))

                except:
                    inb = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initiative buying calculation is not possible")


                # Close relative to today's value area
                try:
                    if filename.loc[pk, 'close'] < filename.loc[pk, 'val']:
                        pz1 = -1
                    elif (filename.loc[pk, 'close'] < filename.loc[pk, 'vah'] and filename.loc[pk, 'close'] > filename.loc[pk, 'val']):
                        pz1 = 0
                    else:
                        pz1 = 1

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Relative close to today's value: "+str(pz1))

                except:
                    pz1 = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Relative close to today's value calculation is not possible")


                # Close relative to previous day's value area
                try:
                    if filename.loc[pk, 'close'] < filename.loc[pm, 'val']:
                        pz2 = -1
                    elif (filename.loc[pk, 'close'] < filename.loc[pm, 'vah'] and filename.loc[pk, 'close'] > filename.loc[pm, 'val']):
                        pz2 = 0
                    else:
                        pz2 = 1

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Relative close to previous's value: "+str(pz2))

                except:
                    pz2 = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Relative close to previous's value calculation is not possible")


                # Trend Day
                try:
                    if filename.loc[pk, 'tpomax'] < 6:
                        trday = 1
                    else:
                        trday = 0

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Trend day: "+str(trday))

                except:
                    trday = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Trend day calculation is not possible")


                # Value Area Placement
                try:

                    if filename.loc[pk, 'val'] > filename.loc[pm, 'vah']:
                        vap = 2
                    elif filename.loc[pk, 'vah'] < filename.loc[pm, 'val']:
                        vap = -2
                    elif (filename.loc[pk, 'vah'] < filename.loc[pm, 'vah'] and filename.loc[pk, 'val'] > filename.loc[pm, 'val']):
                        vap = 0
                    elif (filename.loc[pk, 'vah'] > filename.loc[pm, 'vah'] and filename.loc[pk, 'val'] > filename.loc[pm, 'val'] and filename.loc[pk, 'val'] < filename.loc[pm, 'vah']):
                        vap = 1
                    elif (filename.loc[pk, 'val'] < filename.loc[pm, 'val']) and filename.loc[pk, 'vah'] < filename.loc[pm, 'vah'] and filename.loc[pk, 'vah'] > filename.loc[pm, 'val']:
                        vap = -1
                    elif (filename.loc[pk, 'vah'] > filename.loc[pm, 'vah'] and filename.loc[pk, 'val'] < filename.loc[pm, 'val']):
                        vap = 0
                    else:
                        vap = 0

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Value area placement: "+str(vap))

                except:
                    vap = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Value area placement calculation is not possible")


                # Defining if period min is below the previous day's VAL
                def pervolval(rmini):
                    if rmini < filename.loc[pm, 'val']:
                        return 1
                    else:
                        return 0
                try:
                    rvol2 = pervolval(rmin2)
                    rvol3 = pervolval(rmin3)
                    rvol4 = pervolval(rmin4)
                    rvol5 = pervolval(rmin5)
                    rvol6 = pervolval(rmin6)
                    rvol7 = pervolval(rmin7)
                    rvol8 = pervolval(rmin8)
                    rvol9 = pervolval(rmin9)
                    rvol10 = pervolval(rmin10)
                    rvol11 = pervolval(rmin11)
                    rvol12 = pervolval(rmin12)
                    rvol13 = pervolval(rmin13)

                except:
                    print("[" + time.strftime("%H:%M:%S") + "] " + "All min calculation is not possible")


                # Rotation factor sum
                rfs2 = rf1 + rfm1
                rfs3 = rf2 + rfm2
                rfs4 = rf3 + rfm3
                rfs5 = rf4 + rfm4
                rfs6 = rf5 + rfm5
                rfs7 = rf6 + rfm6
                rfs8 = rf7 + rfm7
                rfs9 = rf8 + rfm8
                rfs10 = rf9 + rfm9
                rfs11 = rf10 + rfm10
                rfs12 = rf11 + rfm11
                rfs13 = rf12 + rfm12


                # Responsive buying.
                '''
                If the given day's particular 30 min range's min is below the previous day's VAL and the 30 min range's rotation factor is positive
                '''
                def respvol(rvoli, rfsi, volperi):
                    if (rvoli == 1 and rfsi > 0):
                        return volperi
                    else:
                        return 0
                try:
                    vav2 = respvol(rvol2, rfs2, volper2)
                    vav3 = respvol(rvol3, rfs3, volper3)
                    vav4 = respvol(rvol4, rfs4, volper4)
                    vav5 = respvol(rvol5, rfs5, volper5)
                    vav6 = respvol(rvol6, rfs6, volper6)
                    vav7 = respvol(rvol7, rfs7, volper7)
                    vav8 = respvol(rvol8, rfs8, volper8)
                    vav9 = respvol(rvol9, rfs9, volper9)
                    vav10 = respvol(rvol10, rfs10, volper10)
                    vav11 = respvol(rvol11, rfs11, volper11)
                    vav12 = respvol(rvol12, rfs12, volper12)
                    vav13 = respvol(rvol13, rfs13, volper13)

                    respb = vav2 + vav3 + vav4 + vav5 + vav6 + vav7 + vav8 + vav9 + vav10 + vav11 + vav12 + vav13

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Responsive buying: "+str(respb))

                except:
                    respb = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Responsive buying calculation is not possible")


                # Initiative Selling
                '''
                If the given day's particular 30 min range's min is below the previous day's VAL and the 30 min range's rotation factor is negative
                '''
                def inselvol(rvoli, rfsi, volperi):
                    if (rvoli == 1 and rfsi < 0):
                        return volperi
                    else:
                        return 0
                try:
                    vak2 = inselvol(rvol2, rfs2, volper2)
                    vak3 = inselvol(rvol3, rfs3, volper3)
                    vak4 = inselvol(rvol4, rfs4, volper4)
                    vak5 = inselvol(rvol5, rfs5, volper5)
                    vak6 = inselvol(rvol6, rfs6, volper6)
                    vak7 = inselvol(rvol7, rfs7, volper7)
                    vak8 = inselvol(rvol8, rfs8, volper8)
                    vak9 = inselvol(rvol9, rfs9, volper9)
                    vak10 = inselvol(rvol10, rfs10, volper10)
                    vak11 = inselvol(rvol11, rfs11, volper11)
                    vak12 = inselvol(rvol12, rfs12, volper12)
                    vak13 = inselvol(rvol13, rfs13, volper13)

                    invs = vak2 + vak3 + vak4 + vak5 + vak6 + vak7 + vak8 + vak9 + vak10 + vak11 + vak12 + vak13

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initiative selling: "+str(invs))

                except:
                    invs = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initiative selling calculation is not possible")


                # Defining if period max is above the previous day's VAH
                def respselvol(rmaxi):
                    if rmaxi > filename.loc[pm, 'vah']:
                        return 1
                    else:
                        return 0
                try:
                    kvol2 = respselvol(rmax2)
                    kvol3 = respselvol(rmax3)
                    kvol4 = respselvol(rmax4)
                    kvol5 = respselvol(rmax5)
                    kvol6 = respselvol(rmax6)
                    kvol7 = respselvol(rmax7)
                    kvol8 = respselvol(rmax8)
                    kvol9 = respselvol(rmax9)
                    kvol10 = respselvol(rmax10)
                    kvol11 = respselvol(rmax11)
                    kvol12 = respselvol(rmax12)
                    kvol13 = respselvol(rmax13)

                except:

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Responsive selling calculation is not possible")


                #Responsive selling
                '''
                If the given day's particular 30 min range's max is above the previous day's VAH and the 30 min range's rotation factor is negative
                '''
                def kavvol(kvoli, rfsi, volperi):
                    if (kvoli == 1 and rfsi < 0):
                        return volperi
                    else:
                        return 0
                try:
                    kav2 = kavvol(kvol2, rfs2, volper2)
                    kav3 = kavvol(kvol3, rfs3, volper3)
                    kav4 = kavvol(kvol4, rfs4, volper4)
                    kav5 = kavvol(kvol5, rfs5, volper5)
                    kav6 = kavvol(kvol6, rfs6, volper6)
                    kav7 = kavvol(kvol7, rfs7, volper7)
                    kav8 = kavvol(kvol8, rfs8, volper8)
                    kav9 = kavvol(kvol9, rfs9, volper9)
                    kav10 = kavvol(kvol10, rfs10, volper10)
                    kav11 = kavvol(kvol11, rfs11, volper11)
                    kav12 = kavvol(kvol12, rfs12, volper12)
                    kav13 = kavvol(kvol13, rfs13, volper13)

                    kavsum = kav2 + kav3 + kav4 + kav5 + kav6 + kav7 + kav8 + kav9 + kav10 + kav11 + kav12 + kav13

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Responsive selling: " + str(kavsum))

                except:
                    kavsum = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Responsive selling calculation is not possible")


                #Initial buying
                '''
                If the given day's particular 30 min range's max is above the previous day's VAH and the 30 min range's rotation factor is positive
                '''
                def mavvol(kvoli, rfsi, volperi):
                    if (kvoli == 1 and rfsi > 0):
                        return volperi
                    else:
                        return 0
                try:
                    mav2 = mavvol(kvol2, rfs2, volper2)
                    mav3 = mavvol(kvol3, rfs3, volper3)
                    mav4 = mavvol(kvol4, rfs4, volper4)
                    mav5 = mavvol(kvol5, rfs5, volper5)
                    mav6 = mavvol(kvol6, rfs6, volper6)
                    mav7 = mavvol(kvol7, rfs7, volper7)
                    mav8 = mavvol(kvol8, rfs8, volper8)
                    mav9 = mavvol(kvol9, rfs9, volper9)
                    mav10 = mavvol(kvol10, rfs10, volper10)
                    mav11 = mavvol(kvol11, rfs11, volper11)
                    mav12 = mavvol(kvol12, rfs12, volper12)
                    mav13 = mavvol(kvol13, rfs13, volper13)

                    mavsum = mav2 + mav3 + mav4 + mav5 + mav6 + mav7 + mav8 + mav9 + mav10 + mav11 + mav12 + mav13

                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initial buying: "+str(mavsum))

                except:
                    mavsum = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Initial buying calculation is not possible")

                try:
                    if (mavsum == 0 and kavsum == 0):
                        bratio = 0
                    elif (mavsum > 0 and kavsum == 0):
                        bratio = round(mavsum / 0.01, 2)
                    else:
                        bratio = round(mavsum / kavsum, 2)

                    if (invs == 0 and respb == 0):
                        sratio = 0
                    elif (invs > 0 and respb == 0):
                        sratio = round(invs / 0.01, 2)
                    else:
                        sratio = round(invs / respb, 2)

                    rbvol = respb * sumvol
                    isvol = invs * sumvol
                    ibvol = mavsum * sumvol
                    rsvol = kavsum * sumvol

                except:
                    bratio = 0
                    sratio = 0
                    rbvol = 0
                    isvol = 0
                    ibvol = 0
                    rsvol = 0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Ratio calculation is not possible")


                # Daily return
                try:
                    dret1 = filename['close']
                    dret2 = np.asarray(dret1)
                    dret = round(((dret2[1] - dret2[0]) / dret2[0]) * 100, 2)

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Daily Return: ' + str(dret) + '%')

                except:
                    dret = 0.0
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Daily return calculation is not possible")


                #Writing daily_rel data to database
                if data_check["daily_rel"].values == "Yes":

                    #Forces dailycalc to save daily_rel data to database
                    if args.force_save_drel == "Yes":

                        # Writing general daily data to database
                        SQL(data_base,args.db_user_name,args.db_password).insert_data(
                            "insert into daily_rel (ins,inb,pz1,pz2,trday,resbuy,inisell,respsell,inibuy,vap,dret,ticker,date,rbvol,isvol,ibvol,rsvol,bratio,sratio) "
                            "values ('" + str(ins) +
                            "','" + str(inb) +
                            "','" + str(pz1) +
                            "','" + str(pz2) +
                            "','" + str(trday) +
                            "','" + str(respb) +
                            "','" + str(invs) +
                            "','" + str(kavsum) +
                            "','" + str(mavsum) +
                            "','" + str(vap) +
                            "','" + str(dret) +
                            "','" + str(security) +
                            "','" + str(query_date) +
                            "','" + str(rbvol) +
                            "','" + str(isvol) +
                            "','" + str(ibvol) +
                            "','" + str(rsvol) +
                            "','" + str(bratio) +
                            "','" + str(sratio) +"')")

                        print("[" + time.strftime("%H:%M:%S") + "] " + "WRITING DAILY_REL DATA TO DATABASE")

                    else:

                        print("[" + time.strftime("%H:%M:%S") + "]" + " DAILY_REL DATA EXISTS IN DATA BASE FOR -> " + str(security) + " DATE: " + str(query_date))

                        pass

                else:

                    # Writing general daily data to database
                    SQL(data_base,args.db_user_name,args.db_password).insert_data(
                        "insert into daily_rel (ins,inb,pz1,pz2,trday,resbuy,inisell,respsell,inibuy,vap,dret,ticker,date,rbvol,isvol,ibvol,rsvol,bratio,sratio) "
                        "values ('" + str(ins) +
                        "','" + str(inb) +
                        "','" + str(pz1) +
                        "','" + str(pz2) +
                        "','" + str(trday) +
                        "','" + str(respb) +
                        "','" + str(invs) +
                        "','" + str(kavsum) +
                        "','" + str(mavsum) +
                        "','" + str(vap) +
                        "','" + str(dret) +
                        "','" + str(security) +
                        "','" + str(query_date) +
                        "','" + str(rbvol) +
                        "','" + str(isvol) +
                        "','" + str(ibvol) +
                        "','" + str(rsvol) +
                        "','" + str(bratio) +
                        "','" + str(sratio) + "')")

                    print("[" + time.strftime("%H:%M:%S") + "] " + "WRITING DAILY_REL DATA TO DATABASE")

                    SQL(data_base,args.db_user_name,args.db_password).insert_data("update process_monitor set daily_rel = 'Yes' where ticker = '" + str(security) + "' and date = '" + str(query_date) + "'")


                '''print("[" + time.strftime("%H:%M:%S") + "] " + '<<< WRITING ROUND 2 DATA TO EXCEL >>>')
                print('')
                # Writing data to database

                wb = load_workbook(datapathexcel)
                ws1 = wb.active

                # rowe = int(ws1['A1'].value)
                try:
                    dat17 = ws1.cell(row=rowe, column=18, value=ins)
                    dat18 = ws1.cell(row=rowe, column=19, value=inb)
                    dat19 = ws1.cell(row=rowe, column=20, value=pz1)
                    dat20 = ws1.cell(row=rowe, column=21, value=pz2)
                    dat21 = ws1.cell(row=rowe, column=22, value=trday)
                    dat22 = ws1.cell(row=rowe, column=23, value=respb)
                    dat23 = ws1.cell(row=rowe, column=24, value=invs)
                    dat24 = ws1.cell(row=rowe, column=25, value=kavsum)
                    dat25 = ws1.cell(row=rowe, column=26, value=mavsum)
                    dat26 = ws1.cell(row=rowe, column=27, value=vap)
                    dat44 = ws1.cell(row=rowe, column=44, value=dret)
                except:
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Writing level 2 data is not possible")
                wb.save(datapathexcel)
                wb.close()

                print('*****Round 3 Data Calculation*****')
                print('')

                # Round3 Data Calculation.........................................................................................

                # EE Index
                try:
                    filename = pd.read_excel(datapathexcel).tail(3)

                    poc11 = np.asanyarray(filename['POC'])
                    poc1 = poc11[0]
                    poc22 = np.asanyarray(filename['POC'])
                    poc2 = poc22[1]
                    poc33 = np.asanyarray(filename['POC'])
                    poc3 = poc33[2]

                    pocd1 = poc2 - poc1
                    pocd2 = poc3 - poc2
                    pocd1abs = abs(pocd1)
                    pocd2abs = abs(pocd2)
                    pocrel1 = pocd1abs / poc1
                    pocrel2 = pocd2abs / poc2
                    pocrel = pocrel2 / pocrel1
                    pocs1 = pocrel + 1
                    pocs2 = 100 / pocs1
                    pocd10 = round(100 - pocs2, 2)

                    if pocd10 == 0:
                        pocd10 = 1

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'EE Index: ', pocd10)

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Calculating --> Balanced Day')

                    # Balanced day

                    if pocd10 < 30:
                        bald = poc
                        edge = rowe
                    else:
                        bald = 0
                        edge = 0

                    # Attempted direction

                    atdir = bre + sre + res + rf + tail + ins + inb + pz1 + pz2 + vap

                    if atdir > 0:
                        atdircode = 1
                    else:
                        atdircode = 2
                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Attempted Direction: ', atdircode)
                    # VAP code

                    if vap == 2:
                        vapcode = 6
                    elif vap == -2:
                        vapcode = 7
                    elif vap == 1:
                        vapcode = 8
                    elif vap == -1:
                        vapcode = 9
                    else:
                        vapcode = 10
                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Value Area Placement code: ', vapcode)
                    shortvol = sumvol - (rbvol + isvol + ibvol + rsvol)

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Writing to excel round 3 calculated data')

                    wb = load_workbook(datapathexcel)
                    ws1 = wb.active
                    ws2 = wb.get_sheet_by_name('Unit')

                    dat27 = ws1.cell(row=rowe, column=28, value=pocd10)
                    dat28 = ws1.cell(row=rowe, column=29, value=bald)
                    dat29 = ws1.cell(row=rowe, column=30, value=tail)
                    dat30 = ws1.cell(row=rowe, column=31, value=atdir)
                    dat31 = ws1.cell(row=rowe, column=32, value=atdircode)
                    dat32 = ws1.cell(row=rowe, column=33, value=edge)
                    dat33 = ws1.cell(row=rowe, column=34, value=bratio)
                    dat34 = ws1.cell(row=rowe, column=35, value=sratio)
                    dat35 = ws1.cell(row=rowe, column=36, value=rbvol)
                    dat36 = ws1.cell(row=rowe, column=37, value=isvol)
                    dat37 = ws1.cell(row=rowe, column=38, value=ibvol)
                    dat38 = ws1.cell(row=rowe, column=39, value=rsvol)
                    dat39 = ws1.cell(row=rowe, column=40, value=shortvol)
                    dat40 = ws1.cell(row=rowe, column=41, value=vapcode)

                    wb.save(datapathexcel)
                    wb.close()
                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Level 3 data was written to excel')

                except:
                    print("[" + time.strftime("%H:%M:%S") + "] " + "Level 3 data calculation is not possible")

                # Relative Volume by Other timefram trader volume

                file1 = pd.read_excel(datapathexcel, sheetname="Sheet").tail(1)
                file33 = pd.read_excel(datapathexcel, sheetname="Unit")
                file3 = file33.tail(1)

                if len(file33['OPEN']) > 0:

                    relavol1 = int(file1['RB VOL'] + file1['IS VOL'] + file1['IB VOL'] + file1['RS VOL'])
                    relativevol1 = np.asanyarray(round(int(relavol1 - file3['OTVOL']) / file3['OTVOL'], 2))
                    relativevol = relativevol1[0]

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Relative Volume: ', relativevol)

                    # Relative Volume code

                    if (relativevol > -0.10 and relativevol < 0.10):
                        relvolcode = 4
                    elif relativevol > 0.10:
                        relvolcode = 5
                    else:
                        relvolcode = 3

                    atpercode = int(str(atdircode) + str(relvolcode) + str(vapcode))
                    atdirtxt = sit[sit.A == atpercode]
                    atdirtxt1 = np.asanyarray(atdirtxt.B)
                    atdirtxt2 = atdirtxt1[-1]

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Attempted Direction Performance: ', atdirtxt2)

                    wb = load_workbook(datapathexcel)
                    ws1 = wb.active

                    dat41 = ws1.cell(row=rowe, column=42, value=relativevol)
                    dat42 = ws1.cell(row=rowe, column=43, value=atdirtxt2)

                    print("[" + time.strftime("%H:%M:%S") + "] " + 'Relative Volume: ', relativevol)

                    wb.save(datapathexcel)
                    wb.close()'''

            except:
                print('')
                print("[" + time.strftime("%H:%M:%S") + "] " + '--------------------------------------------------------> Error')
                print("[" + time.strftime("%H:%M:%S") + "] " + i + ' Production failed')

                print('----------------------------------------')
        print("")
        print('END OF DAILYCALC')
        print("")