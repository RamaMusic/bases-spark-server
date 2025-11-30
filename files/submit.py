import socket
import sys

HOST = 'localhost'
PORT = 9999

def main():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            
            # Receive data until connection closes
            while True:
                data = s.recv(4096)
                if not data:
                    break
                print(data.decode('utf-8'), end='')
                
    except ConnectionRefusedError:
        print("Error: Could not connect to Spark Server. Is it running?")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
