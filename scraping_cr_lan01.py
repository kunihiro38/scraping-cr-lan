# coding:utf-8
import csv
import time
import mysql.connector
import requests, bs4
import chromedriver_binary
# API
import pickle
import os.path
import base64
# scraping
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from urllib.request import(
    urlopen,
    Request,
)
from datetime import datetime
# API
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# Gmail APIのスコープを設定
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
# API
def lancers():
    # lancersのシステム開発案件URLの取得
    driver = webdriver.Chrome()
    driver.get("https://www.lancers.jp/work/search/system/websystem")
    res = requests.get("https://www.lancers.jp/work/search/system/websystem")
    time.sleep(5)
    search_box = driver.find_element_by_id("Keyword")
    search_box.send_keys("python")
    button = driver.find_element_by_id("Search")
    driver.execute_script('arguments[0].click();', button) # execute_scriptでJavaScriptを使ってbuttonをクリック
    time.sleep(5)
    """
    #selectを使用したパターン
    sort_element = driver.find_element_by_class_name("p-search__sort-selectbox")
    #選択したいエレメントをSelectタグに対応したエレメントに変化させる
    sort_select_element = Select(sort_element)
    #選択したいvalueを指定する
    sort_select_element.select_by_value("/work/search/system/websystem?keyword = python&search = %E6%A4%9C%E7%B4%A2&sort = started&work_rank%5B%5D = 0&work_rank%5B%5D = 2&work_rank%5B%5D = 3")
    """
    # seleniumを使用したパターン
    selectbox = driver.find_element_by_class_name("p-search__sort-selectbox")
    selectbox.send_keys("新着順")
    time.sleep(5)
    soup = bs4.BeautifulSoup(driver.page_source, "html.parser")
    time.sleep(5)
    driver.quit()

    with open("lancers.csv", mode="a", encoding="utf-16") as f:
        global current_time
        current_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        writer = csv.writer(f, dialect="excel-tab", delimiter="\t")
        data_array = []
        search_results = soup.find_all("div", attrs = {"class": "c-media-list c-media-list--forClient"})

        for job_data in search_results:
            # 各案件を抽出
            for item in job_data.find_all("div", attrs={"class": "c-media__content__right"}):
                # タイトルの抽出
                global  item_title_l
                item_title_l = item.find("a", attrs={"c-media__title"})
                # 金額を抽出
                global payment_l
                payment_l = item.find("span", attrs={"class": "c-media__job-price"})
                # URLの抽出(<a c-media__titleのタグのhrefの値を取得する )
                rel_url = item.find("a", attrs={"c-media__title"}).get("href")
                global abs_url_l
                abs_url_l = urljoin(res.url, rel_url)
                data_array.append(current_time)
                data_array.append(item_title_l.text.strip())
                data_array.append(payment_l.text.strip())
                data_array.append(abs_url_l)
                # 1行分のデータをCSVに書き出し
                writer.writerow(data_array)
                data_array = []
        f.close()
        print("lancers_done")

def crowdworks():
    # クラウドワークスのシステム開発案件URLの取得
    res = requests.get('https://crowdworks.jp/public/jobs/group/development')
    res.raise_for_status()
    soup = bs4.BeautifulSoup(res.text, "html.parser")
    driver = webdriver.Chrome()
    driver.get('https://crowdworks.jp/public/jobs/group/development')
    time.sleep(1)
    search_box = driver.find_element_by_name("search[keywords]")
    search_box.send_keys("python")
    search_box.submit()
    time.sleep(5)
    soup = bs4.BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    with open("crowdworks.csv", mode="a", encoding="utf-16") as f:
        current_time = datetime.now().strftime("%Y/%m/%d %H: %M: %S")
        writer = csv.writer(f, dialect="excel-tab", delimiter="\t")
        data_array = []
        search_results = soup.find_all("div", attrs={"class": "search_results"})

        for job_data in search_results:
            # 各案件を抽出
            for item in job_data.find_all('li', attrs={"data-jobs-list-item": True}):
                # タイトルの抽出
                global item_title_c
                item_title_c = item.find("h3", attrs={"class": "item_title"})
                # 金額の抽出
                global payment_c
                payment_c = item.find("div", attrs={"class": "entry_data payment"})
                # URLの抽出(<a data-item-title-linkのタグのhrefの値を取得する )
                rel_url = item.find("a", attrs={"data-item-title-link": True}).get("href")
                # URLの合成
                global abs_url_c
                abs_url_c = urljoin(res.url, rel_url)
                data_array.append(current_time)
                data_array.append(item_title_c.text.strip())
                data_array.append(payment_c.text.strip())
                data_array.append(abs_url_c)
                # 1行分のデータをCSVに書き出し
                writer.writerow(data_array)
                data_array = []
        f.close()
        print("crowdworks_done")

def create_mysql():
    # MySQL接続へ接続
    url = urlparse('mysql://root:@localhost:3306/mysql')
    conn = mysql.connector.connect(
        host = url.hostname or 'localhost',
        port = url.port or 3306,
        user = url.username or 'root',
        password = url.password or '',
        database = "scraping_job" # 存在するDB名入力
    )
    print(conn.is_connected())
    cur = conn.cursor()
    cur.execute('show databases')
    cur.fetchall()
    """
    #定義の確認
    #テーブルの削除（繰り返し見れるように）
    crowdworks = "drop table crowdworks"
    cur.execute(crowdworks)
    print("crowdworksテーブルを削除しました!")
    #crowdworksテーブルの作成
    crowdworks = "create table crowdworks (\
        created datetime,\
        demand varchar(255),\
        price varchar(255),\
        url varchar(255)\
        )"
    cur.execute(crowdworks)
    print("*crowdworksテーブルを作成しました!\n")
    #テーブルの削除（繰り返し見れるように）
    lancers = "drop table lancers"
    cur.execute(lancers)
    print("crowdworksテーブルを削除しました!")
    # lancersテーブルの作成
    lancers = "create table lancers (\
        created datetime,\
        demand varchar(255),\
        price varchar(255),\
        url varchar(255)\
        )"
    cur.execute(sql2)
    print("*lancersテーブルを作成しました!\n")

    # cur.execute("show tables")
    # cur.execute("show tables")
    # print("==テーブル一覧の確認==")
    # print(cur.fetchall())

    sql_make = "select * from crowdworks"
    sql_make = "select * from lancers"

    # sql_make_1 = "desc crowdworks"
    # sql_make_2 = "desc lancers"
    cur.execute(sql_make_1)
    cur.execute(sql_make_2)
    print(cur.fetchall())
    print("定義の確認をしました")
    """
    # MySQLのcrowdworksテーブルに保存
    crowdworks = "insert into crowdworks(created, demand, price, url) values(%s, %s, %s, %s)"
    cur.execute(crowdworks, (current_time, item_title_c.text, payment_c.text, abs_url_c))
    conn.commit()

    # MySQLのlancersテーブルに保存
    lancers = "insert into lancers(created, demand, price, url) values(%s, %s, %s, %s)"
    cur.execute(lancers, (current_time, item_title_l.text, payment_l.text, abs_url_l))
    conn.commit()
# メール作成
def create_message(sender, to, subject, message_text, attach_file):
    message = MIMEMultipart()
    message["to"] = to
    message["from"] = sender
    message["subject"] = subject
    msg = MIMEText(message_text)
    message.attach(msg)
    try:
        # ファイルの種類の定義
        msg1 = MIMEBase("text", "comma-separated-values")
        msg2 = MIMEBase("text", "comma-separated-values")
        file_location_c = os.path.abspath("crowdworks.csv")
        file_location_l = os.path.abspath("lancers.csv")
        attachment_c = open(file_location_c, "rb")
        attachment_l = open(file_location_l, "rb")

        msg1.set_payload((attachment_c).read())
        msg2.set_payload((attachment_l).read())
        encoders.encode_base64(msg1)
        encoders.encode_base64(msg2)
        msg1.add_header(f"Content-Disposition", "attachment", filename="crowdworks.csv")
        msg2.add_header(f"Content-Disposition", "attachment", filename="lancers.csv")

        message.attach(msg1)
        message.attach(msg2)

    except:
        print("There is no file here")

    # encode bytes string
    byte_msg = message.as_string().encode(encoding="UTF-8")
    byte_msg_b64encoded = base64.urlsafe_b64encode(byte_msg)
    str_msg_b64encoded = byte_msg_b64encoded.decode(encoding="UTF-8")
    return {"raw": str_msg_b64encoded}

# メール送信の実行
def send_message(service, user_id, message, attach_file):
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
        .execute())
        print('Message Id: %s' % message['id'])
        return message
    except errors.HttpError as error:
        print('An error occurred: %s' % error)

# メール送信メインとなる処理
def main_msg():
    creds = None # アクセストークンの取得。
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
            # 認証完了後トークンを取得
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(\
                 "credentials-gmail.json", SCOPES)
            creds = flow.run_local_server()

        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
         # GmailAPIを利用するためのサービス作成
    service = build("gmail", "v1", credentials=creds)
    # メール本文の作成
    global to
    to = "送信先メールアドレス記載"
    sender = "送信側メールアドレス記載"
    subject = "スクレイピングのメール送信自動化テスト"
    message_text = "自動化のテストをしています"
    attach_file = {"name1": "lancers.csv", "name2": "crowdworks.csv"}

    message = create_message(sender, to, subject, message_text, attach_file)
    # Gmail APIを呼び出してメール送信
    send_message(service, "me", message , attach_file)

if __name__ == "__main__":
    while True: # 永久に実行
        if datetime.now().second == 1: # ○時△分1秒以外の時には作動し続ける
            continue
        lancers()
        crowdworks()
        create_mysql()
        main_msg()
        print("all done")
        time.sleep(60*60*12) # 12時間指定
