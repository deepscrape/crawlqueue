import os
from dotenv import load_dotenv
import firebase_admin.auth
import firebase_admin.db
import firebase_admin.firestore
import firebase_admin.storage
import firebase_admin
from firebase_admin import credentials
import json

# Load environment variables
load_dotenv()
firestoreConfig = json.loads(os.getenv("FIRESTORE_CONFIG"))
firebase_credentials = json.loads(os.getenv("FIREBASE_CREDENTIALS"))


class FirebaseClient:
    def __init__(self):
        self.db = None
        self.auth = None
        self.firestoreConfig = firestoreConfig
        self.firebase_credentials = firebase_credentials

        # Load Firebase credentials
        self.company_credentials = credentials.Certificate(self.firebase_credentials)

    def init_firebase(self):
        # # Initialize Firebase if not already initialized
        if not firebase_admin._apps:
            # Initialize Firebase app with the company credentials
            firebase_admin.initialize_app(self.company_credentials)

        # Set up Firestore with a custom database ID
        dbName = firestoreConfig["dbName"]  # Replace with your Firestore database ID

        self.db = firebase_admin.firestore.client(database_id=dbName)

        # Assign the authentication object
        self.auth = firebase_admin.auth

        # Print a message indicating that Firestore has been initialized
        print(
            f"\033[94mINFO:\033[0m     \033[92mFirestore initialized by\033[0m \033[95m{dbName}\033[0m \033[94mdatabase\033[0m pid: {os.getpid()} {self.db}"
        )

        return (self.db, self.auth)


client: FirebaseClient = FirebaseClient()
# usage
db, auth = client.init_firebase()
