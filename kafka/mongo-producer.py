from pymongo import MongoClient
from confluent_kafka import Producer
import json
import socket
from bson import ObjectId
# Kết nối tới MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Thay đổi chuỗi kết nối nếu cần

#config producer
topics = ['spark']
topic = 'spark'
conf = {
    'bootstrap.servers': 'localhost:9094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024',
    'client.id' : socket.gethostname(),
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 5,
}

producer = Producer(**conf)

# Chọn cơ sở dữ liệu và bộ sưu tập
db = client['mydatabase']  # Thay đổi tên cơ sở dữ liệu
collection = db['summary']  # Thay đổi tên bộ sưu tập

page_size = 1000  # Số lượng tài liệu mỗi lần truy vấn
page = 0

# push data to producer
MIN_FLUSH_COUNT = 500
producer_msg_count =0
def json_serializable(doc):
    if isinstance(doc, ObjectId):
        return str(doc)  # Chuyển đổi ObjectId thành chuỗi
    raise TypeError("Type not serializable")
def acked(err, msg):
        if err is not None:
            # print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            print("Failed to deliver message: %s" % str(err))
        else:
            # print("Message produced: %s" % (str(msg)))
            ("load data successfully")


def producer_process(msg_json):
    global producer_msg_count
    producer_msg_count +=1
    json_data = json.dumps(msg_json,default=json_serializable)
    producer.produce(topic,value = json_data,callback = acked)
    if(producer_msg_count%MIN_FLUSH_COUNT==0):
        producer.flush()


while True:
    data = collection.find().skip(page * page_size).limit(page_size)
    documents = list(data)

    if not documents:  # Nếu không còn tài liệu nào
        break

    for document in documents:
        producer_process(document)

    page += 1  # Chuyển sang trang tiếp theo
    print(f"page thu: {page}")
