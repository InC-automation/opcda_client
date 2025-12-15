import grpc
import elecont_pb2
import elecont_pb2_grpc
import time
from opcda_exchange import opcda_exchange
from grpc_exchange import grpc_exchange

opcda_exchange = opcda_exchange()
grpc_exchange = grpc_exchange()

try:
    while True:        
        tag_names = grpc_exchange.get_tag_names()
        data_pool = opcda_exchange.read_tags(tag_names)
        grpc_exchange.write_to_cs(data_pool)
        commands = grpc_exchange.get_commands()        
        opcda_exchange.write_to_opcda(commands)
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    print("Finish...")
