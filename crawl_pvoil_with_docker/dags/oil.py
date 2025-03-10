import datetime
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import time
import pandas as pd
import mysql.connector


def ket_noi_mysql():
    mydb = mysql.connector.connect(
        host='docker-db-1',
        user='root',
        password='ecotruckmysql',
        database='newdb',
        port= "3306"
    )
    return mydb


def tao_bang(mydb):
    bang = f"""CREATE TABLE IF NOT EXISTS  pvoil(
        name varchar(255),
        price_oil float,
        price_gap float,
        thoi_gian date

    )
    """

    mycursor = mydb.cursor()
    mycursor.execute(bang)


def lay_gia_dau(mydb):

    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors')
    driver = webdriver.Remote(
    command_executor= 'http://172.19.0.3:4444',
    options=options
    )



    # http://localhost:4444/wd/hub
    

    # options = webdriver.ChromeOptions()

    # options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # driver = webdriver.Chrome(
    #     service=Service(executable_path="chromedriver.exe"),
    #     options=options,
    # )

    time.sleep(5)
    url = "https://www.pvoil.com.vn/truyen-thong/tin-gia-xang-dau"
    driver.get(url)
    driver.maximize_window()

    # Lấy tất cả ngày tháng trong pvoil
    result = requests.get(url)
    doc = BeautifulSoup(result.text, "html.parser")

    mydivs = doc.find_all("div", {"class": "select-form"})
    for i in mydivs:
        ds_date = "".join(i.text.strip())
    ds_date = ds_date.split("\n")

    # Lấy mốc thời gian trên pvoil (thư viện bs4)
    thead = doc.thead
    thead = thead.find_all("strong")
    thoi_gian = [g.string.strip() for g in thead[2:3]]

    list_of_dataframes = []
    sl = 4
    for i in range(0, len(ds_date)-1):
        ddelement = Select(driver.find_element(
            By.ID, 'ctl00_mainContent_ctl03_ddlDate'))
        list_datetime = ddelement.select_by_index(i)
        time.sleep(5)
        name = driver.find_elements(By.XPATH, '//tbody/tr/td[2]')
        price_oil = driver.find_elements(By.XPATH, '//tbody/tr/td[3]')
        price_gap = driver.find_elements(By.XPATH, '//tbody/tr/td[4]')

        ds_name = [i.text for i in name[:4]]
        ds_price_oil = [i.text for i in price_oil[:4]]
        ds_price_gap = [i.text for i in price_gap[:4]]

        thoi_gian = driver.find_elements(
            By.XPATH, '//thead/tr[1]/td[3]/span[1]/strong')
        ds_thoi_gian = [i.text for i in thoi_gian[:4]]
        ds_thoi_gian = "".join(ds_thoi_gian).split()[6]
        ds_thoi_gian = [datetime.datetime.strptime(
            ds_thoi_gian, '%d/%m/%Y').strftime('%Y-%m-%d')]

        # in ra dataframe
        dict = {'name': ds_name, 'price_oil': ds_price_oil,
                'price_gap': ds_price_gap, 'thoi_gian': ds_thoi_gian[0]}
        df = pd.DataFrame(dict)
        list_of_dataframes.append(df)
        time.sleep(5)
        dataf = pd.concat(list_of_dataframes, ignore_index=True)
    # print(dataf)

    # dataf.to_csv('output.csv', index=False)

        for i in range(4):
            val = (ds_name[i], ds_price_oil[i],
                   ds_price_gap[i], ds_thoi_gian[0])
            mycursor = mydb.cursor()
            sql = "insert into pvoil(name, price_oil, price_gap, thoi_gian ) VALUES (%s, %s, %s, %s)"
            mycursor.execute(sql, val)
            mydb.commit()
        # # print(mycursor.rowcount, "completed record")

    dataf.to_csv('/opt/airflow/dags/output.csv', index=False)

    driver.quit()

def output_airflow_2():
    mydb = ket_noi_mysql()
    tao_bang(mydb)
    lay_gia_dau(mydb)

if __name__ == "__main__":   
    output_airflow_2()
