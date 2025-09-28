"""
consumer_hanson.py

Consume json messages from a Kafka topic (topic: buzzline_hanson, group: buzz_hanson). 
Insert the processed messages into a database.
Analyze sentiment scores by author in real time.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Processed Results:
- Update sentiment scores by author in SQLite database (hanson.sqlite)
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
import sqlite3

# import external modules
from kafka import KafkaConsumer

# import from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

######################################
# PATHS
######################################

BASE_DIR = pathlib.Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)  # make sure data folder exists

SQLITE_PATH = DATA_DIR / "hanson.sqlite"

TOPIC = "buzzline_hanson"
GROUP_ID = "buzz_hanson"
KAFKA_URL = "127.0.0.1:9092"


# Ensure the parent directory is in sys.path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from consumers.sqlite_consumer_case import init_db, insert_message

#####################################
# Database Setup
#####################################

def init_db(db_path: pathlib.Path) -> None:
    """Initialize the SQLite database with the required table."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT,
            sentiment REAL
        )
        """
    )
    conn.commit()
    conn.close()

#####################################
# Update sentiment by author
######################################

def update_sentiment_by_author(author: str, sentiment: float, db_path: pathlib.Path) -> None:
    """Update the sentiment score for a given author in the database."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO messages (author, sentiment)
        VALUES (?, ?)
        """,
        (author, sentiment),
    )
    conn.commit()
    conn.close()

#####################################
# Function to process a single message
# #####################################

def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
    processed_message = {
        "message": message.get("message"),
        "author": message.get("author"),
        "timestamp": message.get("timestamp"),
        "category": message.get("category"),
        "sentiment": float(message.get("sentiment", 0.0)),
        "keyword_mentioned": message.get("keyword_mentioned"),
        "message_length": int(message.get("message_length", 0)),
    }

    update_sentiment_by_author(
        processed_message["author"], processed_message["sentiment"], SQLITE_PATH
    )




#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka():
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.
    """
    logger.info("Called consume_messages_from_kafka()")

    
    logger.info("Starting to consume messages...")
    
    # Step 1: Verify Kafka service is available
    logger.info("Step 1. Verify Kafka services are available.")
    verify_services()

    # Step 2: Create Kafka consumer
    consumer = create_kafka_consumer(
        TOPIC,
        GROUP_ID,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8"))
    )

    # Step 3: Check if topic exists
    logger.info("Step 3. Check if topic exists.")
    is_topic_available(TOPIC)

    # Step 4: Process messages in real-time
    logger.info("Step 4. Begin consuming and processing messages.")
    for msg in consumer: 
        process_message(msg.value)










#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting consumer_hanson.")
    if SQLITE_PATH.exists():
        SQLITE_PATH.unlink()
        logger.info("Deleted old database.")

    init_db(SQLITE_PATH)
    consume_messages_from_kafka()


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
