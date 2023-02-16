import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector

r = requests.get("https://www.finanzen.net/indizes/alle")
soup = BeautifulSoup(r.text,'html.parser')

url_list = []
for i in range(0,627):
    try:
        url_list.append(str("https://www.finanzen.net")+soup.find_all('tbody',{'class':'table__tbody'})[1].find_all('tr')[i].find('a').get('href'))
    except:
        continue


stock_list = []
for i in range(len(url_list)):
    u = url_list[i]
    r_2 = requests.get(u)
    soup_2 = BeautifulSoup(r_2.text,'html.parser')
    # print(i)
    for i in range(0,50):
        try:
            stock_list.append(str("https://www.finanzen.net/bilanz_guv/")+soup_2.find_all('div',{'class':'horizontal-scrolling table--content-right'})[0].find_all('tbody')[0].find_all('a')[i].get('href')[8:-6])
        except:
            break


cnx = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "Python@12",
    database = "testing"
)

for i in range(1,len(stock_list)):
    r = requests.get(stock_list[i])
    c = r.content
    tabels = pd.read_html(c)
    print("Storing Data of ", stock_list[i][36:].upper()," ",i)
    try:
        for j in range(1,8):
            data = []
            cl = tabels[2].columns
            cln = cl[j]
            for table_num in range(1,6):
                # tabels[table_num].drop('Unnamed: 0',inplace=True,axis=1)
                for k, row in tabels[table_num].iterrows():
                    data.append(row[j])
            cursor = cnx.cursor()
            cursor.execute("INSERT INTO `output_able` (`Stock`, `Year`, `Ergebnis je Aktie (unverwässert, nach Steuern)`, `Ergebnis je Aktie (verwässert, nach Steuern)`, `Dividende je Aktie`, `Gesamtdividendenausschüttung in Mio.`, `Umsatz je Aktie`, `KGV (Jahresendkurs)`, `KGV (Jahresendkurs, EPS verwässert)`, `Dividendenrendite Jahresende in %`, `Eigenkapitalquote in %`, `Fremdkapitalquote in %`, `Umsatzerlöse`, `Umsatzveränderung in %`, `Bruttoergebnis vom Umsatz`, `Bruttoergebnisveränderung in %`, `Operatives Ergebnis`, `Veränderung Operatives Ergebnis in %`, `Ergebnis vor Steuern`, `Veränderung Ergebnis vor Steuern in %`, `Ergebnis nach Steuer`, `Veränderung Ergebnis nach Steuer in %`, `Gesamtverbindlichkeiten`, `Langzeit Gesamtverbindlichk. pro Aktie`, `Eigenkapital`, `Veränderung Eigenkapital in %`, `Bilanzsumme`, `Veränderung Bilanzsumme in %`, `Gewinn je Aktie (unverwässert, nach Steuern)`, `Veränderung EPS (unverwässert) in %`, `Gewinn je Aktie (verwässert, nach Steuern)`, `Veränderung EPS (verwässert) in %`, `Veränderung Dividende je Aktie in %`, `Anzahl Mitarbeiter`, `Veränderung Anzahl Mitarbeiter in %`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",(stock_list[i][36:].upper(), str(cln),str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[5]),str(data[6]),str(data[7]),str(data[8]),str(data[9]),str(data[10]),str(data[11]),str(data[12]),str(data[13]),str(data[14]),str(data[15]),str(data[16]),str(data[17]),str(data[18]),str(data[19]),str(data[20]),str(data[21]),str(data[22]),str(data[23]),str(data[24]),str(data[25]),str(data[26]),str(data[27]),str(data[28]),str(data[29]),str(data[30]),str(data[31]),str(data[32])))
            cnx.commit()
    except:
        continue
