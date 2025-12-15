# библиотека OpenOPC
import OpenOPC
import pythoncom
import pywintypes
pywintypes.datetime = pywintypes.TimeType
from datetime import datetime, timezone
import time
import configparser

# класс для работы с OPC DA
class opcda_exchange:    
    opcda_connect_status = False
    opcda_link = OpenOPC.client()  # локальный клиент
    opcda_server_name = ''
    sync_mode = False
    tags_limit = 0
    trace = False
    
    # инициализация класса
    def __init__(self):     
        config = configparser.ConfigParser()
        config.read('settings.ini')
        self.opcda_server_name = config['Default']['OPCDA_SERVER']
        self.sync_mode = bool(int(config['Default']['SYNC_MODE']))
        self.tags_limit = int(config['Default']['TAGS_LIMIT'])        
        self.trace = bool(int(config['Default']['TRACE']))

    # метод установливает соединение с OPC DA (вызывается при ошибке соединения)
    def opcda_connect(self):  
        if self.opcda_connect_status: return
            
        print(f'{datetime.now().time()} OPC DA connect ({self.opcda_server_name})...')
        try:
            self.opcda_link.connect(self.opcda_server_name)
        except Exception as e:  # Если ошибка, заркываем соединение и ждем 5 с
            print(f'{datetime.now().time()} OPC DA connect ({self.opcda_server_name}) failed: {e}')
            self.opcda_close(5)
        else:
            if self.trace: print(f'{datetime.now().time()} OPC DA connect SUCCESS')
            self.opcda_connect_status = True
            time.sleep(0.2)

    # метод читает теги OPC DA (opcda_tag_names - список имен тегов на чтение)
    def read_tags(self, opcda_tag_names):
        if not opcda_tag_names:     # если нет тегов для чтения, закрываем соединение и выходим
            if self.trace: print(f'{datetime.now().time()} OPC DA nothing to read...')
            self.opcda_close(0.2)
            return []
        if not self.opcda_connect_status: # если нет соединения с сервером OPC DA, выходим
            self.opcda_connect()
            return []
        if self.trace: print(f'{datetime.now().time()} OPC DA read tags...')        
        
        try:
            # чтение тегов
            opcda_tags = self.opcda_link.read(opcda_tag_names, sync = self.sync_mode, timeout = 1000, size = self.tags_limit)
        except Exception as e:
            print(f'{datetime.now().time()} OPC DA Read error occurred: {e}')
            self.opcda_close(1)
            return []
        else:
            return opcda_tags

    # метод записывает теги OPC DA (command_pool - список тегов на запись (<имя тега>, <значение>))
    def write_to_opcda(self, command_pool):       # записываем данные в сервер OPC DA
        if not self.opcda_connect_status: # если нет соединения с сервером OPC DA, выходим
            self.opcda_connect()
            return []        
        if self.trace: print(f'{datetime.now().time()} OPC DA write ({len(command_pool)} values)...')
        if command_pool:
            try:
                self.opcda_link.write(command_pool)
            except Exception as e:
                print(f'{datetime.now().time()} OPC DA Write error occurred: {e}')
                self.opcda_close(1)        
            time.sleep(0.2)

    # метд закрывает соединение OPC DA
    def opcda_close(self, tSleep = 0):
        print(f'{datetime.now().time()} Close OPC DA connect...')
        self.opcda_connect_status = False
        try:
            self.opcda_link.close()
        except:
            pass            
        time.sleep(tSleep)

    # финализация класса
    def __del__(self):  
        print(f'{datetime.now().time()} Close OPC DA connect (final)...')
        try:
            self.opcda_link.close()
        except:
            pass