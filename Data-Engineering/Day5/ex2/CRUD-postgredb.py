import os
import asyncio
import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
# Read database connection parameters from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

async def get_connection():
    """Get a database connection"""
    return await asyncpg.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

async def create_table():
    """Create the users table if it doesn't exist"""
    conn = await get_connection()
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        await conn.close()

async def insert_data(name, email):
    """Insert a new user into the database"""
    conn = await get_connection()
    try:
        user_id = await conn.fetchval('''
            INSERT INTO users (name, email) 
            VALUES ($1, $2) 
            RETURNING id
        ''', name, email)
        print(f"User inserted successfully with ID: {user_id}")
        return user_id
    except Exception as e:
        print(f"Error inserting data: {e}")
        return None
    finally:
        await conn.close()

async def read_data(user_id=None):
    """Read user data from the database"""
    conn = await get_connection()
    try:
        if user_id:
            user = await conn.fetchrow('''
                SELECT * FROM users WHERE id = $1
            ''', user_id)
            if user:
                print(f"User: {user}")
            else:
                print(f"No user found with ID: {user_id}")
            return user
        else:
            users = await conn.fetch('''
                SELECT * FROM users
            ''')
            print(f"Total users: {len(users)}")
            for user in users:
                print(user)
            return users
    except Exception as e:
        print(f"Error reading data: {e}")
        return None
    finally:
        await conn.close()

async def update_data(user_id, name=None, email=None):
    """Update user data in the database"""
    conn = await get_connection()
    try:
        update_parts = []
        params = []
        
        if name:
            update_parts.append("name = $1")
            params.append(name)
        
        if email:
            update_parts.append("email = $2")
            params.append(email)
        
        if not update_parts:
            print("No fields to update")
            return False
        
        params.append(user_id)
        query = f'''
            UPDATE users 
            SET {', '.join(update_parts)} 
            WHERE id = ${len(params)}
        '''
        
        result = await conn.execute(query, *params)
        if "UPDATE 1" in result:
            print(f"User with ID {user_id} updated successfully")
            return True
        else:
            print(f"No user found with ID: {user_id}")
            return False
    except Exception as e:
        print(f"Error updating data: {e}")
        return False
    finally:
        await conn.close()

async def delete_data(user_id):
    """Delete a user from the database"""
    conn = await get_connection()
    try:
        result = await conn.execute('''
            DELETE FROM users WHERE id = $1
        ''', user_id)
        if "DELETE 1" in result:
            print(f"User with ID {user_id} deleted successfully")
            return True
        else:
            print(f"No user found with ID: {user_id}")
            return False
    except Exception as e:
        print(f"Error deleting data: {e}")
        return False
    finally:
        await conn.close()

async def main():
    # Create the table
    await create_table()
    
    # Insert 10 sample data records
    user_ids = []
    sample_users = [
        ("John Doe", "john@example.com"),
        ("Jane Smith", "jane@example.com"),
        ("Michael Johnson", "michael@example.com"),
        ("Emily Davis", "emily@example.com"),
        ("Robert Wilson", "robert@example.com"),
        ("Sarah Brown", "sarah@example.com"),
        ("David Miller", "david@example.com"),
        ("Jennifer Taylor", "jennifer@example.com"),
        ("Thomas Anderson", "thomas@example.com"),
        ("Lisa Martinez", "lisa@example.com")
    ]
    
    for name, email in sample_users:
        user_id = await insert_data(name, email)
        if user_id:
            user_ids.append(user_id)
    
    # Read all users
    print("\nAll users:")
    await read_data()
    
    # Read a specific user
    if user_ids:
        print("\nSpecific user:")
        await read_data(user_ids[0])
    
    # Update a user
    if user_ids:
        print("\nUpdating user:")
        await update_data(user_ids[0], name="John Updated")
        await read_data(user_ids[0])
    
    # Delete a user
    if len(user_ids) > 1:
        print("\nDeleting user:")
        await delete_data(user_ids[1])
    
    # Verify deletion
    print("\nAfter deletion:")
    await read_data()

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
