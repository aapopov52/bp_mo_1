import pika
import json
import pandas as pd
import os
 
try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
   
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
    # 
    df = pd.DataFrame(columns=['id', 'y_true', 'y_pred', 'absolute_error'])
    # 
    def df_insert_data(id, y_true, y_pred):
        global df
        if (df['id'] == id).any():
            if y_true is not None:
                df.loc[df['id'] == id, 'y_true'] = y_true
            if y_pred is not None:
                df.loc[df['id'] == id, 'y_pred'] = y_pred
            # y_pred дожен появться несколько позднее y_true
            # вместес тем потенциально возможны сбои (прежде всего в брокере),
            # и, как следствие, нарушение этого очевидного пордяка
            # поэтому вставим строку в этой части кода (без привязки к вносимому значению)
            flag = (df['id'] == id) & (df['y_true'].isna() == False) & (df['y_pred'].isna() == False)
            # теоретически df может быть весьма большим
            # поэтому создадим маленикий датафрейм в одну запись
            # выполним необходимые действия
            # а большоve df будем обращаться 2 раза
            # - первый - при создании малого датафремй
            # - второй - при удалении сохранённой строки (она нам более не понадобится)
            df_save = df[flag]
            if len(df_save) > 0:
                df_save['absolute_error'] = abs(df_save['y_true'] - df_save['y_pred'])
                
                file_path = 'logs/metric_log.csv'
                if not os.path.isfile(file_path):
                    df_save.to_csv(file_path, mode='w', index=False, header=True)  # Создаём файл, включая заголовок
                else:
                    df_save.to_csv(file_path, mode='a', index=False, header=False) # Дописываем существующий файл
                
                df = df[~flag]
        else:
            df = df.append({'id': id, 'y_true': y_true, 'y_pred': y_pred}, ignore_index=True)

    # Создаём функцию callback для обработки данных из очереди
    def callback_true(ch, method, properties, body):
        features = json.loads(body)
        df_insert_data(id = features['id'], y_true = features['body'], y_pred = None)
    
    def callback_pred(ch, method, properties, body):
        features = json.loads(body)
        df_insert_data(id = features['id'], y_true = None, y_pred = features['body'])        
 
    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback_true,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback_pred,
        auto_ack=True
    )
 
    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')
