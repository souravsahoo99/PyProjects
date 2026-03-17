import os
from pymongo import MongoClient
from dotenv import load_dotenv
from dotenv import find_dotenv
dotenvfile: str = find_dotenv()
load_dotenv(dotenvfile)



CONNECTION_STR = os.getenv("CONNECTION_STRING")
# MongoDB connection string

mongo_client = MongoClient(CONNECTION_STR)
students = mongo_client['AlgoBot']['TradeLogs']

# CREATE: Insert a new student
student = {
    "name": "John Doe",
    "age": 21,
    "courses": ["Math", "Science", "History"]
}

student_id = students.insert_one(student).inserted_id
print(f"Inserted student with id: {student_id}")

student = {
    "name": "Sugam Gupta",
    "age": 35,
    "courses": ["Python", "Machine Learning", "AI"]
}
students.insert_one(student)


# READ: Find the student by name
found_student = students.find_one({"name": "John Doe"})
print("Found student:", found_student)




# UPDATE: Update the student's age
update_result = students.update_one(
    {"name": "John Doe"},
    {"$set": {"age": 25}}
)
print(f"Updated {update_result.modified_count} document(s)")




# READ: Verify the update
updated_student = students.find_one({"name": "John Doe"})
print("Updated student:", updated_student)




# DELETE: Remove the student
delete_result = students.delete_one({"name": "John Doe"})
print(f"Deleted {delete_result.deleted_count} document(s)")


