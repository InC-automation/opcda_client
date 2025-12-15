import grpc
import elecont_pb2
import elecont_pb2_grpc
from datetime import datetime, timezone, timedelta
import time
import configparser

quality_dict = {
'GOOD': 	        0,
'LOCAL_OVERRIDE': 	1024,
'UNCERTAIN': 	    3,
'SUB_NORMAL': 	    3,
'SENSOR_CAL': 	    515,
'EGU_EXCEEDED': 	7,
'LAST_USABLE': 	    131,
'BAD': 	            2,
'CONFIG_ERROR': 	66,
'NOT_CONNECTED': 	66,
'COMM_FAILURE': 	66,
'OUT_OF_SERVICE': 	66,
'DEVICE_FAILURE': 	66,
'SENSOR_FAILURE': 	66,
'WAITING_FOR_INITIAL_DATA': 	2,
'ERROR':            66,
}

TIME_PATTERN_1 = '%Y-%m-%d %H:%M:%S.%f%z'       #2025-08-13 13:14:16.691000+00:00
TIME_PATTERN_2 = '%Y-%m-%d %H:%M:%S%z'          #2025-08-21 12:06:04+00:00

class grpc_exchange:
    sig_dict = {}        # Словарь {guid:userdata} (for RX_SIGNAL)
    sig_values = {}      # Словарь {guid:signal}   (for RX_SIGNAL)
    com_dict = {}        # Словарь {guid:userdata} (for TX_COMMAND)
    com_values = {}      # Словарь {guid:comvalue} (for TX_COMMAND)
    tag_dict = {}        # Словарь {userdata:guid} (for RX_SIGNAL)
    opc_read_tag_names = []
    grpc_connect_status = False
    grpc_channel = grpc.insecure_channel('localhost:29040')
    stub = elecont_pb2_grpc.ElecontStub(grpc_channel)
    grpc_url = ''
    cycle_period = 10
    connect_period = 30
    time_delta = 0    
    trace = False

    # инициализатор класса (чтение параметров работы приложения)
    def __init__(self): 
        config = configparser.ConfigParser()
        config.read('settings.ini')
        self.grpc_url = config['Default']['ELECONT_GRPC']        
        self.trace = bool(int(config['Default']['TRACE']))
        self.cycle_period = float(config['Default']['CYCLE_PERIOD'])
        self.connect_period = float(config['Default']['CONNECT_PERIOD'])
        self.time_delta = int(config['Default']['TIME_DELTA'])
    
    # метод установливает соединение gRCPC и читает данные (сигналы и команды)
    def grcp_connect(self):          
        if self.grpc_connect_status: return
        print(f'{datetime.now().time()} grcp_connect ({self.grpc_url})...')
        self.grpc_channel = grpc.insecure_channel(self.grpc_url)
        self.stub = elecont_pb2_grpc.ElecontStub(self.grpc_channel)
        
        try:
            cs_data = self.stub.GetAllObjectsData(elecont_pb2.Empty())
        except grpc.RpcError as e:
            print(f'{datetime.now().time()} GetAllObjectsData gRPC error: {e.code()}, {e.details()}')
            self.grcp_close(5)
            #return []
        else:
            if self.trace: print(f'{datetime.now().time()} gRPC connect SUCCESS')
            self.grpc_connect_status = True
            self.set_dicts(cs_data)

    # метод заполняет словари сигналов/команд: tag_dict, sig_values, com_dict, com_values и список тегов на чтение opc_read_tag_names
    # метод вызывается после установки соединения gRCP
    def set_dicts(self, cs_data):   
        if self.trace: print(f'{datetime.now().time()} set_dicts...')
        self.sig_dict.clear()
        self.sig_values.clear()
        self.tag_dict.clear()
        self.opc_read_tag_names.clear()
        self.com_dict.clear()
        self.com_values.clear()
        
        for cs_obj in cs_data.data:
            if elecont_pb2.ObjectFamily.Value.Name(cs_obj.family.value) == 'RX_SIGNAL':
                self.sig_dict[cs_obj.guid] = cs_obj.userdata
                try:
                    self.sig_values[cs_obj.guid] = self.stub.GetSignalByGuid(elecont_pb2.Guid(guid = cs_obj.guid))
                except grpc.RpcError as e:
                    print(f'{datetime.now().time()} GetSignalByGuid gRPC error: {e.code()}, {e.details()}')
                    self.grcp_close(5)  
                self.tag_dict[cs_obj.userdata] = cs_obj.guid
                self.opc_read_tag_names.append(cs_obj.userdata)   
                #print(f'Tag: {cs_obj.guid} {cs_obj.userdata}')
            if elecont_pb2.ObjectFamily.Value.Name(cs_obj.family.value) == 'TX_COMMAND':
                self.com_dict[cs_obj.guid] = cs_obj.userdata
                self.com_values[cs_obj.guid] = ''

    def get_tag_names(self):
        return self.opc_read_tag_names
    
    # метод преобразует текстовую метку времени сигнала в timestamp в формате КС
    def get_timestamp(self, time_string = None):  
        tz = timezone(timedelta(hours=self.time_delta))
        if time_string == None:
            new_time = datetime.now().replace(tzinfo = tz)
        else:
            if '.' in time_string:
                time_pattern = TIME_PATTERN_1
            else:
                time_pattern = TIME_PATTERN_2
            new_time = datetime.strptime(time_string, time_pattern).replace(tzinfo = tz)
        timestamp = int(new_time.timestamp() * 1000)
        sec = int(timestamp/1000)
        ms = timestamp % 1000   
        return (sec | (ms << 33))
    
    # метод записывает значения тегов в сигналы КС (SetSignal)
    def write_to_cs(self, opcda_tags):          
        if self.trace: print(f'{datetime.now().time()} write_to_cs...')
        global TIME_PATTERN_1, TIME_PATTERN_2
        if not self.grpc_connect_status: 
            self.grcp_connect()
            return
        if not opcda_tags:
            self.write_bad_values()
            return
        
        for tag_name, tag_value, tag_quality, time_string in opcda_tags:
            signal = self.sig_values[self.tag_dict[tag_name]]
            #if tag_value == None or time_string == None or tag_quality == 'Error':
            if tag_quality == 'Error':
                if self.trace: print(f'{datetime.now().time()} Error read the tag: {tag_name}')
                tag_value = signal.value

            new_value = str(tag_value)
            if signal.type.value == elecont_pb2.ElecontSignalType.BOOLEAN:
                if new_value.lower() == 'true':
                    new_value = '1'
                else:
                    new_value = '0'            
           
            new_quality = quality_dict[tag_quality.upper()]
            new_timestamp = self.get_timestamp(time_string)
            
            # записать сигнал в КС, если изменилось значение или качество
            if signal.value != new_value or signal.quality != new_quality:    
                signal.value = new_value
                signal.quality = new_quality
                signal.time = new_timestamp
                self.sig_values[signal.guid] = signal
                try:
                    self.stub.SetSignal(signal)
                except grpc.RpcError as e:
                    print(f'{datetime.now().time()} SetSignal gRPC error: {e.code()}, {e.details()}')
                    self.grcp_close(5)
                    return
        time.sleep(self.cycle_period/1000)     # задержка перед следующим циклом
        
    # метод приcваивает сигналы с плохим значением качества (вызывается при обрыве соединение с OPC DA)
    def write_bad_values(self):
        if not self.grpc_connect_status: return
        if self.trace: print(f'{datetime.now().time()} write_bad_values...')
        
        if self.grpc_connect_status:   # приcвоить плохое значение качества
            for item in self.sig_values:
                signal = self.sig_values[item]
                if signal.quality != quality_dict['DEVICE_FAILURE']:
                    signal.quality = quality_dict['DEVICE_FAILURE']
                    signal.time = self.get_timestamp()
                    self.sig_values[signal.guid] = signal
                    try:
                        self.stub.SetSignal(signal)
                    except grpc.RpcError as e:
                        print(f'{datetime.now().time()} gRPC error: {e.code()}, {e.details()}')
                        self.grcp_close(5)   # закрыть соединение, если ошибка
                        return
    
    # метод закрывает соединение gRCP
    def grcp_close(self, tSleep = 0):
        print(f'{datetime.now().time()} Close gRPC connect...')
        self.grpc_connect_status = False
        try:
            self.grpc_channel.close()
        except:
            pass
        self.opc_read_tag_names.clear()
        time.sleep(tSleep)
    
    def __del__(self):
        print(f'{datetime.now().time()} Close gRPC connect (final)...')
        try:
            self.grpc_channel.close()
        except:
            pass

    # метод читает команды КС и возращается список объектов типа (<Имя тего OPC UA>,<значение>)
    def get_commands(self): # прочитать команды
        if not self.grpc_connect_status: return
        if self.trace: print(f'{datetime.now().time()} get_commands...')
        command_pool = []
        command_guids = self.com_dict.keys()           
        try:
            commands = self.stub.GetCommandsByGuid(elecont_pb2.SignalsGuid(guid = command_guids))
        except grpc.RpcError as e:
            print(f'{datetime.now().time()} get_commandsByGuid gRPC error: {e.code()}, {e.details()}')
            self.grcp_close(5)   # закрыть соединение, если ошибка
            return
        if not commands: return
      
        for command in commands.int8_command:
            tag_name_value = self.process_command(command, 'int8')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int16_command:
            tag_name_value = self.process_command(command, 'int16')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int32_command:
            tag_name_value = self.process_command(command, 'int32')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int64_command:
            tag_name_value = self.process_command(command, 'int64')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int8u_command:
            tag_name_value = self.process_command(command, 'int8u')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int16u_command:
            tag_name_value = self.process_command(command, 'int16u')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int32u_command:
            tag_name_value = self.process_command(command, 'int32u')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.int64u_command:
            tag_name_value = self.process_command(command, 'int64u')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.float32_command:
            tag_name_value = self.process_command(command, 'float32')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.float64_command:
            tag_name_value = self.process_command(command, 'float64')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.visible_string255_command:
            tag_name_value = self.process_command(command, 'string')
            if tag_name_value: command_pool.append(tag_name_value)
        for command in commands.boolean_command:
            tag_name_value = self.process_command(command, 'boolean')
            if tag_name_value: command_pool.append(tag_name_value)
        return command_pool  
        
    # метод преобразует команду в соответствии с типом данных и возращает объект типа (<Имя тего OPC UA>,<значение>)
    def process_command(self, command, data_type):
        command_guid = command.cmdprop.guid
        current_command_value = self.com_values[command_guid]
        if command.value != current_command_value:
            self.com_values[command_guid] = command.value
            tag_name = self.com_dict[command_guid]
            if 'int' in data_type.lower(): tag_value = int(command.value)
            elif 'float' in data_type.lower(): tag_value = float(command.value)
            elif 'bool' in data_type.lower(): tag_value = bool(command.value)
            elif 'string' in data_type.lower(): tag_value = command.value
            return (tag_name, tag_value)
        return    
 
