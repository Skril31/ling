import os
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.mydatabase
collection = db.articles
documents = collection.find()
count_documents = 0
for news in documents:
    print(count_documents)
    if count_documents == 10000:
        break

    f = open('./tomita/input.txt', 'w')
    f.write(news['text'])
    f.close()

    #Start tomita
    os.system("cd tomita/; ./tomita-parser config.proto")

    f = open('./tomita/output.txt', 'r').readlines()

    line = 0
    analiz_text = ""
    while line < len(f):
        str_p = "Person: "
        if f[line].find('Polit') > -1:
            analiz_text += str(f[line - 1][:-1])
            while True:
                str_p += str(f[line + 2][12:-1]) + "|"
                line += 4
                if line >=len(f) or f[line].find('Polit') == -1:
                    break
            str_p += "#\n"
            analiz_text += str_p

        if line >=len(f):
            break

        str_pl = "Place: "
        if f[line].find('Place') > -1:
            analiz_text += str(f[line - 1][:-1])
            while True:
                str_pl += str(f[line + 2][9:-1]) + "|"
                line += 4
                if line >=len(f) or f[line].find('Polit') == -1:
                    break
            str_pl += "#\n"
            analiz_text += str_pl        
        line+=1

    if len(analiz_text) > 0: 
       id_news = news['_id']
       #print(id_news)

       tomita_c = db.tomita
       old_news = tomita_c.find_one_and_delete({'_id': id_news})
       tomita_c.insert_one(
           {
               '_id': id_news,
               'text': analiz_text
           }
       )

    count_documents += 1


print("THE END")
