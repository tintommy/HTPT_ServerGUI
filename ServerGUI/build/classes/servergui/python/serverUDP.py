from dateutil import parser
import threading
import datetime
import socket
import time

# Data structure used to store client address and clock data
client_data = {}

def startReceivingClockTime(master_server):
    while True:
        try:
            # Receive clock time and client address from UDP
            data, addr = master_server.recvfrom(1024)
            clock_time_string = data.decode()
            clock_time = parser.parse(clock_time_string)
            clock_time_diff =  clock_time -datetime.datetime.now()

            slave_address = str(addr[0]) + ":" + str(addr[1])

            client_data[slave_address] = {
                "clock_time": clock_time,
                "time_difference": clock_time_diff,
                "connector": master_server
            }

            print("Client Data updated with: " + slave_address, end="\n\n")
            time.sleep(5)
        except Exception as e:
            print("Error receiving clock time from " + slave_address + ": " + str(e))
            client_data.pop(slave_address)

def startConnecting(master_server):
    while True:
        try:
            # Accepting a client/slave clock client for UDP
            data, addr = master_server.recvfrom(1024)  # Receive data and address
            slave_address = str(addr[0]) + ":" + str(addr[1])

            print(slave_address + " got connected successfully")

            current_thread = threading.Thread(
                target=startReceivingClockTime,
                args=(master_server,))
            current_thread.start()
        except Exception as e:
            print("Error accepting connection: " + str(e))

def getAverageClockDiff():
    current_client_data = client_data.copy()

    time_difference_list = list(client['time_difference']
                                for client_addr, client
                                in client_data.items())

    sum_of_clock_difference = sum(time_difference_list, datetime.timedelta(0, 0))
    average_clock_difference = sum_of_clock_difference / (len(client_data)+1)

    return average_clock_difference

def synchronizeAllClocks():
    while True:
        print("New synchronization cycle started.")
        print("Number of clients to be synchronized: " + str(len(client_data)))

        if len(client_data) > 0:
            average_clock_difference = getAverageClockDiff()

            for client_addr, client in client_data.items():
                try:
                    synchronized_time = datetime.datetime.now() + average_clock_difference
                    client['connector'].sendto(str(synchronized_time).encode(), (client_addr.split(':')[0], int(client_addr.split(':')[1])))
                except Exception as e:
                    print("Error sending synchronized time through " + client_addr + ": " + str(e))

        else:
            print("No client data. Synchronization not applicable.")

        print("\n\n")
        time.sleep(5)

def initiateClockServerUDP(port=8080):
    master_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
    master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("========> " + str(master_server.type))
    print("Socket at master node created successfully\n")

    master_server.bind(('', port))
    print("Clock server started...\n")

    # Start handling clients
    print("Starting to handle clients...\n")
    handle_clients_thread = threading.Thread(
        target=startConnecting,
        args=(master_server,))
    handle_clients_thread.start()

    # Start synchronization
    print("Starting synchronization parallelly...\n")
    sync_thread = threading.Thread(
        target=synchronizeAllClocks,
        args=())
    sync_thread.start()

# Driver function
if __name__ == '__main__':
    # Trigger the Clock Server
    initiateClockServerUDP(port=8080)
