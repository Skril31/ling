import requests
from pymongo import MongoClient
from dateutil import parser
from datetime import datetime
import os
from bs4 import BeautifulSoup


def clear_col(collection_name):
    collection_name.delete_many({})

def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb://localhost:27017/"
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)
    return client['sem']

def get_news_urls(page):
    soup = BeautifulSoup(page, 'lxml')
    news_items = soup.find_all("li", class_="news-listing__item")
    links=[]
    for item in news_items:
        links.append(item.find("a", class_="news-listing__item-link").get("href"))
    return links

def get_page_data(page):
    soup = BeautifulSoup(page.text, 'lxml')
    date = datetime.fromisoformat(soup.find("time", class_="meta__text").get("datetime").split("+")[0]).strftime("%d.%m.%Y %H:%M")
    #date = parser.parse(soup.find("time", class_="meta__text").get("datetime"))
    views = int(soup.find("span", class_="meta__item_views").find("span", class_="meta__text").text)
    title = soup.find("h1", class_="article__title").text
    article_text = ""
    if(soup.find("div", class_="article__description") is not None):
        if(soup.find("div", class_="article__description").find("p") is not None):
            article_text += soup.find("div", class_="article__description").find("p").text
        else:
            article_text+=soup.find("div", class_="article__description").find("strong").text
    paragraphs = soup.find("div", class_="article__body").find_all("p")
    for paragraph in paragraphs:
        article_text+=" "+paragraph.text
    return [date, views, title, article_text]

def collect_data(collection_name):
    #ua = UserAgent()
    data = {'text': 'google',
            'lr': '51'}
    headers = {
        'Accept': 'text / html, application / xhtml + xml, application / xml; q = 0.9, image / avif, image / webp, image / apng, * / *;q = 0.8, application / signed - exchange; v = b3; q = 0.9',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:26.0) Gecko/20100101 Firefox/26.0'
    }
    news_num=0
    for month in range (3,11):
        long_month=[3,5,7,8,10]
        if(month in long_month):
            month_len=31+1
        else:
            month_len=30+1
        for day in range(1,month_len):
            url = f'https://volg.mk.ru/news/2022/{month}/{day}/'
            response = requests.get(url=url, headers=headers, params=data)
            urls=get_news_urls(response.text)
            for url in urls:
                news_page=requests.get(url=url, headers=headers, params=data)
                date, views, title, article_text=get_page_data(news_page)
                if(collection_name.count_documents({"link": url})==0):
                    news_num += 1
                    item={
                        "_id":news_num,
                        "title":title,
                        "date":date,
                        "views":views,
                        "text":article_text,
                        "link":url
                    }
                    collection_name.insert_one(item)
                    print(f"Номер новости: {news_num}, дата: {date}")
                else:
                    print(f"(дата {date})Повтор :{url} ")


def main():
    db = get_database()
    collection_name = db["news"]
    #clear_col(collection_name)
    collect_data(collection_name)

if __name__ == '__main__':
    main()