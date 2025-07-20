from flask import Flask, request, jsonify
import sqlite3
import json
from datetime import datetime
from collections import deque

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Database initialization
def init_db():
    conn = sqlite3.connect('social_network.db')
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            internal_db_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_str_id TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create connections table - stores mutual friendships
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user1_internal_id INTEGER,
            user2_internal_id INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user1_internal_id) REFERENCES users (internal_db_id),
            FOREIGN KEY (user2_internal_id) REFERENCES users (internal_db_id),
            UNIQUE(user1_internal_id, user2_internal_id)
        )
    ''')
    
    # Create indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_connections_user1 ON connections (user1_internal_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_connections_user2 ON connections (user2_internal_id)')
    
    conn.commit()
    conn.close()

# Helper function to get database connection
def get_db():
    conn = sqlite3.connect('social_network.db')
    conn.row_factory = sqlite3.Row
    return conn

# Helper function to get user by string ID
def get_user_by_str_id(user_str_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_str_id = ?', (user_str_id,))
    user = cursor.fetchone()
    conn.close()
    return user

# Helper function to ensure consistent connection ordering (smaller ID first)
def order_connection(id1, id2):
    return (id1, id2) if id1 < id2 else (id2, id1)

# 1. POST /users - Create a new user
@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json()
    
    if not data or 'user_str_id' not in data or 'display_name' not in data:
        return jsonify({'error': 'user_str_id and display_name are required'}), 400
    
    user_str_id = data['user_str_id']
    display_name = data['display_name']
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            'INSERT INTO users (user_str_id, display_name) VALUES (?, ?)',
            (user_str_id, display_name)
        )
        internal_db_id = cursor.lastrowid
        conn.commit()
        
        response = {
            'internal_db_id': internal_db_id,
            'user_str_id': user_str_id,
            'status': 'created'
        }
        
        conn.close()
        return jsonify(response), 201
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': 'User already exists'}), 409
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

# 2. POST /connections - Create a mutual connection between two users
@app.route('/connections', methods=['POST'])
def create_connection():
    data = request.get_json()
    
    if not data or 'user1_str_id' not in data or 'user2_str_id' not in data:
        return jsonify({'error': 'user1_str_id and user2_str_id are required'}), 400
    
    user1_str_id = data['user1_str_id']
    user2_str_id = data['user2_str_id']
    
    # Don't allow self-connections
    if user1_str_id == user2_str_id:
        return jsonify({'error': 'Cannot connect user to themselves'}), 400
    
    # Find both users
    user1 = get_user_by_str_id(user1_str_id)
    user2 = get_user_by_str_id(user2_str_id)
    
    if not user1:
        return jsonify({'error': 'User1 not found'}), 404
    if not user2:
        return jsonify({'error': 'User2 not found'}), 404
    
    # Order the connection consistently (smaller ID first)
    id1, id2 = order_connection(user1['internal_db_id'], user2['internal_db_id'])
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            'INSERT INTO connections (user1_internal_id, user2_internal_id) VALUES (?, ?)',
            (id1, id2)
        )
        conn.commit()
        conn.close()
        
        return jsonify({'status': 'connection_added'}), 201
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': 'Connection already exists'}), 409
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

# 3. GET /users/{user_str_id}/friends - Get all direct friends of a user
@app.route('/users/<user_str_id>/friends', methods=['GET'])
def get_friends(user_str_id):
    user = get_user_by_str_id(user_str_id)
    
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get all connections where this user is involved
    cursor.execute('''
        SELECT u.user_str_id, u.display_name
        FROM connections c
        JOIN users u ON (
            (c.user1_internal_id = ? AND u.internal_db_id = c.user2_internal_id) OR
            (c.user2_internal_id = ? AND u.internal_db_id = c.user1_internal_id)
        )
    ''', (user['internal_db_id'], user['internal_db_id']))
    
    friends = cursor.fetchall()
    conn.close()
    
    friends_list = [
        {
            'user_str_id': friend['user_str_id'],
            'display_name': friend['display_name']
        }
        for friend in friends
    ]
    
    return jsonify(friends_list), 200

# 4. DELETE /connections - Remove connection between two users
@app.route('/connections', methods=['DELETE'])
def remove_connection():
    data = request.get_json()
    
    if not data or 'user1_str_id' not in data or 'user2_str_id' not in data:
        return jsonify({'error': 'user1_str_id and user2_str_id are required'}), 400
    
    user1_str_id = data['user1_str_id']
    user2_str_id = data['user2_str_id']
    
    # Find both users
    user1 = get_user_by_str_id(user1_str_id)
    user2 = get_user_by_str_id(user2_str_id)
    
    if not user1:
        return jsonify({'error': 'User1 not found'}), 404
    if not user2:
        return jsonify({'error': 'User2 not found'}), 404
    
    # Order the connection consistently
    id1, id2 = order_connection(user1['internal_db_id'], user2['internal_db_id'])
    
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute(
        'DELETE FROM connections WHERE user1_internal_id = ? AND user2_internal_id = ?',
        (id1, id2)
    )
    
    if cursor.rowcount == 0:
        conn.close()
        return jsonify({'error': 'Connection not found'}), 404
    
    conn.commit()
    conn.close()
    
    return jsonify({'status': 'connection_removed'}), 200

# 5. GET /users/{user_str_id}/friends-of-friends - Get friends of friends (degree 2)
@app.route('/users/<user_str_id>/friends-of-friends', methods=['GET'])
def get_friends_of_friends(user_str_id):
    user = get_user_by_str_id(user_str_id)
    
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    conn = get_db()
    cursor = conn.cursor()
    
    # First, get all direct friends
    cursor.execute('''
        SELECT u.internal_db_id, u.user_str_id, u.display_name
        FROM connections c
        JOIN users u ON (
            (c.user1_internal_id = ? AND u.internal_db_id = c.user2_internal_id) OR
            (c.user2_internal_id = ? AND u.internal_db_id = c.user1_internal_id)
        )
    ''', (user['internal_db_id'], user['internal_db_id']))
    
    direct_friends = cursor.fetchall()
    direct_friend_ids = {friend['internal_db_id'] for friend in direct_friends}
    
    # Now get friends of each direct friend
    friends_of_friends = set()
    
    for friend in direct_friends:
        cursor.execute('''
            SELECT u.internal_db_id, u.user_str_id, u.display_name
            FROM connections c
            JOIN users u ON (
                (c.user1_internal_id = ? AND u.internal_db_id = c.user2_internal_id) OR
                (c.user2_internal_id = ? AND u.internal_db_id = c.user1_internal_id)
            )
        ''', (friend['internal_db_id'], friend['internal_db_id']))
        
        friend_connections = cursor.fetchall()
        
        for connection in friend_connections:
            # Exclude self and direct friends
            if (connection['internal_db_id'] != user['internal_db_id'] and 
                connection['internal_db_id'] not in direct_friend_ids):
                friends_of_friends.add((connection['user_str_id'], connection['display_name']))
    
    conn.close()
    
    fof_list = [
        {
            'user_str_id': user_str_id,
            'display_name': display_name
        }
        for user_str_id, display_name in friends_of_friends
    ]
    
    return jsonify(fof_list), 200

# 6. GET /connections/degree - Calculate degree of separation using BFS
@app.route('/connections/degree', methods=['GET'])
def get_degree_of_separation():
    from_user = request.args.get('from_user_str_id')
    to_user = request.args.get('to_user_str_id')
    
    if not from_user or not to_user:
        return jsonify({'error': 'from_user_str_id and to_user_str_id are required'}), 400
    
    user1 = get_user_by_str_id(from_user)
    user2 = get_user_by_str_id(to_user)
    
    if not user1:
        return jsonify({'error': 'From user not found'}), 404
    if not user2:
        return jsonify({'error': 'To user not found'}), 404
    
    if user1['internal_db_id'] == user2['internal_db_id']:
        return jsonify({'degree': 0}), 200
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Build adjacency list for BFS
    cursor.execute('''
        SELECT user1_internal_id, user2_internal_id FROM connections
    ''')
    
    connections = cursor.fetchall()
    conn.close()
    
    # Create adjacency list
    graph = {}
    for conn in connections:
        id1, id2 = conn['user1_internal_id'], conn['user2_internal_id']
        
        if id1 not in graph:
            graph[id1] = []
        if id2 not in graph:
            graph[id2] = []
        
        graph[id1].append(id2)
        graph[id2].append(id1)
    
    # BFS to find shortest path
    if user1['internal_db_id'] not in graph:
        return jsonify({'degree': -1, 'message': 'not_connected'}), 200
    
    queue = deque([(user1['internal_db_id'], 0)])
    visited = {user1['internal_db_id']}
    
    while queue:
        current_user, degree = queue.popleft()
        
        if current_user == user2['internal_db_id']:
            return jsonify({'degree': degree}), 200
        
        if current_user in graph:
            for neighbor in graph[current_user]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, degree + 1))
    
    return jsonify({'degree': -1, 'message': 'not_connected'}), 200

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)