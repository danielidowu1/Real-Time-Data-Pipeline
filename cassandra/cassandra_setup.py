from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')

# === 1. YOUR ORIGINAL CONNECTION LOGIC (with the Auth fix) ===
def create_connection():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster([CASSANDRA_HOST], port=9042, auth_provider=auth_provider)
    return cluster, cluster.connect()

# === 2. YOUR ORIGINAL KEYSPACE CREATION ===
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    print("Keyspace created")

# === 3. YOUR ORIGINAL TABLE CREATION ===
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    print("Table created")

# === 4. THE NEW WRITE LOGIC ===
def insert_user(session, user_data):
    query = """
        INSERT INTO spark_streams.created_users (
            id, first_name, last_name, gender, address, 
            post_code, email, username, registered_date, phone, picture
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    prepared = session.prepare(query)
    session.execute(prepared, (
        user_data.get('id'),
        user_data.get('first_name'),
        user_data.get('last_name'),
        user_data.get('gender'),
        user_data.get('address'),
        user_data.get('post_code'),
        user_data.get('email'),
        user_data.get('username'),
        user_data.get('registered_date'),
        user_data.get('phone'),
        user_data.get('picture')
    ))
    print(f"User {user_data.get('username')} written successfully!")

# === 5. THE MAIN EXECUTION ENGINE ===
if __name__ == "__main__":
    cluster, session = create_connection()
    
    try:
        # First, run your setup to make sure the database is ready
        create_keyspace(session)
        create_table(session)
        
        # Second, pass real data into the table
        sample_user = {
            "id": "12345-abcde",
            "first_name": "John",
            "last_name": "Doe",
            "gender": "male",
            "address": "123 Main Street",
            "post_code": "90210",
            "email": "johndoe@example.com",
            "username": "johndoe99",
            "registered_date": "2026-05-21",
            "phone": "555-0199",
            "picture": "http://example.com"
        }
        insert_user(session, sample_user)
        
    finally:
        cluster.shutdown()
        print("Connection safely closed.")
