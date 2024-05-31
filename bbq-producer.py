"""
Laura Gagnon-Vos
05/31/2024

"""
import pika
import sys
import webbrowser
import csv
import time
from util_logger import setup_logger
logger, logname = setup_logger(__file__)


def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()

    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()



# Create and send a message to the queue each execution.
"""
Parameters:
    channel (pika.channel.Channel): The communication channel to RabbitMQ.
    queue_name (str): the name of the queue
    message (tuple): the message to be sent to the queue
"""
def send_message(channel, queue_name: str, message: tuple):
    try:
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        channel.basic_publish(exchange="", routing_key=queue_name, body=str(message))
        # print a message to the console for the user
        logger.info(f" [x] Sent message to {queue_name} : {message}")
    except pika.exceptions.AMQPConnectionError as e:
        #print(f"Error: Connection to RabbitMQ server failed: {e}")
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

        
#create function to run main work
def main(hostName):
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(hostName))
        # use the connection to create a communication channel
        ch = conn.channel()
        #delete the existing queues
        ch.queue_delete('01-smoker')
        ch.queue_delete('02-food-A')
        ch.queue_delete('03-food-B')
        # use the channel to declare a durable queue
        ch.queue_declare('01-smoker', durable=True)
        ch.queue_declare('02-food-A', durable=True)
        ch.queue_declare('03-food-B', durable=True)

        #open csv and read it
        with open('smoker-temps.csv', 'r') as file:
            reader = csv.reader(file)
            # Remove the header row
            header = next(reader)
            # for each row in csv declare variable
            for data_row in reader:
                timestamp = data_row[0]
                SmokerTemp = data_row[1]
                Food_A_Temp = data_row[2]
                Food_B_Temp = data_row[3]
                #convert to float if exists, make tuples, and publish message to queues
                if SmokerTemp:
                    SmokerTemp = float(SmokerTemp)
                    Channel1_smoker = (timestamp, SmokerTemp)
                    send_message(ch,"01-smoker",Channel1_smoker)
                if Food_A_Temp:
                    Food_A_Temp = float(Food_A_Temp)
                    Channel2_foodA = (timestamp, Food_A_Temp)
                    send_message(ch,"02-food-A",Channel2_foodA)
                if Food_B_Temp:
                    Food_B_Temp = float(Food_B_Temp) 
                    Channel3_foodB = (timestamp, Food_B_Temp)
                    send_message(ch,"02-food-B",Channel3_foodB)   

                #read one value every half minute
                time.sleep(30)
            

    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = True
    if show_offer is True:
        offer_rabbitmq_admin_site()
    #run main program    
    main('localhost')

    
