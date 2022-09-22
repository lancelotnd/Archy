import base64
import json

from google.cloud.functions.context import Context
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
from google.cloud.firestore_v1.document import DocumentReference, DocumentSnapshot

from requests.adapters  import HTTPAdapter, Retry
import requests

def get_etat_service() -> dict[str,str]:
    """calls the STM api and returns a map of all messages per line"""
    session = requests.Session()
    params = dict(apikey="uwu", origin="www.uqam.ca")
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    url = "https://api.stm.info/pub/od/i3/v1/messages/etatservice/"
    response = session.get(url, headers=params, verify=False)
    data = response.json()
    messages = {}

    for alert in data["alerts"]:
        for line in alert["informed_entities"]:
            if "route_short_name"in line:
                messages[line["route_short_name"]] = alert["header_texts"][0]["text"]

    return messages

def get_previous_state_from_db()-> dict[str,str]:
    """Returns the previous state of metro lines from Firestore"""
    
    lines: dict[str,str]
    
    database: Client = Client(project="archy-f06ed")
    metro_entry: DocumentReference = (
        database.collection("metro").document("all_lines")
    )
    doc_lines: DocumentSnapshot = metro_entry.get()
    lines["1"] = metro_entry.get("1")
    lines["2"] = metro_entry.get("2")
    lines["4"] = metro_entry.get("4")
    lines["5"] = metro_entry.get("5")
    
    return lines


def stm(event: dict, _context: Context):
    """Get STM data to check for status."""

    pubsub_message = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    print(pubsub_message)

    print("Start")

    messages = get_etat_service()
    metro_lines = ["1","2","4","5"]
    metro_state: dict[str,str] = get_previous_state_from_db()

    for line in metro_lines:
        message:str = "Service normal du métro"
        if line in messages:
            message = messages[line]
        if message != metro_state[line]:
            #The state of this metro line has changed
            metro_state[line] = message
            prepare_message(line, message)


    print("Done")
    return "", 200


def send_message(channel_id: str, message: str) -> None:
    """Send a message to discord channel."""

    # Publisher setup
    project_id = "archy-f06ed"
    topic_id = "channel_message_discord"
    publisher = PublisherClient()
    topic_path: str = publisher.topic_path(project_id, topic_id)

    # Data must be a bytestring
    data = {"channel_id": channel_id, "message": message}
    user_encode_data: str = json.dumps(data, indent=2).encode("utf-8")

    # When you publish a message, the client returns a future.
    future: Future = publisher.publish(topic_path, user_encode_data)

    print(f"Message id: {future.result()}")
    print(f"Published message to {topic_path}.")


def prepare_message(line_number:str, message:str) -> None:
    """Prepares a message to send"""
    lines_names = {"1": "Ligne verte",
     "2": "Ligne orange",
     "4": "Ligne jaune",
     "5": "Ligne bleu"}

    to_send:str = lines_names[line_number] + " "+ message
    send_message("", to_send)
