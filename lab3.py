from fxp_bytes_subscriber import Subscriber

if __name__ == '__main__':
    print("testing lab3 subscriber")
    lab3_object = Subscriber()
    lab3_object.subscribe()
    lab3_object.read()
    exit(1)