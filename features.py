import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime

 
# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет о диабете

        X, y = load_diabetes(return_X_y=True)
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0]-1)
 
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
 
        # Создаём очередь y_true
        channel.queue_declare(queue='y_true')
        # Создаём очередь features
        channel.queue_declare(queue='features')
        
        while 1 == 1:
            message_id = datetime.timestamp(datetime.now())
            # Публикуем сообщение в очередь y_true
            message_y_true = {
            'id': message_id,
            'body': y[random_row]
            }
            channel.basic_publish(exchange='',
                                routing_key='y_true',
                                body=json.dumps(message_y_true))
            print('Сообщение с правильным ответом отправлено в очередь')     
            # Публикуем сообщение в очередь features
            message_features = {
            'id': message_id,
            'body': list(X[random_row])
            }

            channel.basic_publish(exchange='',
                                routing_key='features',
                                body=json.dumps(message_features))
            print('Сообщение с вектором признаков отправлено в очередь')
            
            time.sleep(10)
            
    
        # Закрываем подключение
        connection.close()
    except:
        print('Не удалось подключиться к очереди')