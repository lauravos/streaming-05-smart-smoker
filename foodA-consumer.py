"""
Laura Gagnon-Vos
06/07/2024

Consumer for Food A

"""

import pika
import sys
from util_logger import setup_logger 
logger, logname = setup_logger(__file__)
from collections import deque 

# declare constants
# At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 
foodA_deque = deque(maxlen=20)  # limited to 20 items (the 20 most recent readings)

def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")
    convertedTuple = eval(body.decode())
    timestamp = convertedTuple[0]
    Food_A_Temp = float(convertedTuple[1])

    #add temp to deque
    foodA_deque.appendleft(Food_A_Temp)
    #set alert
    if len(foodA_deque) == foodA_deque.maxlen:
        #logger.info(foodA_deque)
        if max(foodA_deque) - min(foodA_deque) < 1:
            logger.info(f" [x] {timestamp}: FOOD A STALL ALERT!!")

    # when done with task, tell the user
    #logger.info(" [x] Done.")
    # acknowledge the message was received and processed 
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hostName: str = "localhost"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostName))

    # except, if there's an error, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hostName}.")
        logger.info(f"The error says: {e}")
        logger.info()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()
        #delete the existing queues
        channel.queue_delete('02-food-A')
        # use the channel to declare a durable queue
        channel.queue_declare('02-food-A', durable=True)


        # Set the prefetch count to one to limit the number of messages being consumed and processed concurrently.     
        channel.basic_qos(prefetch_count=1) 


        # configure the channel to listen on a specific queue, use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue='02-food-A', on_message_callback=foodA_callback, auto_ack=False)
        # print a message to the console for the user
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info()
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
