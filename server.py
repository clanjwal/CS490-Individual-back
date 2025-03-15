from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_mysqldb import MySQL
import MySQLdb.cursors
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)

 
app.config['MYSQL_HOST'] = '127.0.0.1' # might be localhost
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'XFbhjuL2?' # figure out how to hide with env variables later
app.config['MYSQL_DB'] = 'sakila'

mysql = MySQL(app)

# query 6, features 1 and 2
@app.route('/top5movies', methods=['GET'])
def get_top5movies():
    conn = mysql.connection
    cursor = conn.cursor()
    top5 = """
    SELECT F.film_id, F.title, COUNT(R.rental_id) AS rented, F.description, F.release_year, F.rental_rate
    FROM inventory AS I, film AS F, rental AS R
    WHERE F.film_id = I.film_id AND R.inventory_id = I.inventory_id
    GROUP BY F.film_id
    ORDER BY rented DESC
    LIMIT 5;
    """
    
    cursor.execute(top5)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# query 3, feature 3
@app.route('/top5actors', methods=['GET'])
def get_top5actors():
    conn = mysql.connection
    cursor = conn.cursor()
    top5actors = """
    SELECT A.actor_id, A.first_name, A.last_name, COUNT(FA.actor_id) as movies
    FROM actor AS A, film_actor AS FA
    WHERE A.actor_id = FA.actor_id
    GROUP BY A.actor_id
    ORDER BY movies DESC
    LIMIT 5;
    """
    
    cursor.execute(top5actors)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# query 7, feature 4
@app.route('/top5actors_movies', methods=['GET'])
def get_top5actors_movies():
    actor_id = request.args.get("actor_id")
    conn = mysql.connection
    cursor = conn.cursor()

    top5actors_movies = """
    SELECT F.film_id, F.title, COUNT(R.rental_id) AS rental_count
    FROM inventory AS I, film AS F, rental AS R, film_actor AS FA, actor AS A
    WHERE F.film_id = I.film_id AND R.inventory_id = I.inventory_id AND FA.film_id = F.film_id AND A.actor_id = FA.actor_id
    AND FA.actor_id = %s
    GROUP BY F.film_id
    ORDER BY rental_count DESC
    LIMIT 5;
    """
    
    cursor.execute(top5actors_movies, (actor_id,))
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)


# query 1 heavily modified, features 5+6
@app.route('/searchfilms', methods=['GET'])
def search_films():
    search_query = request.args.get("query", "").strip()
    category = request.args.get("category", "title")

    if not search_query:
        return jsonify([])

    conn = mysql.connection
    cursor = conn.cursor()

    # query on category (title/actor/genre)
    if category == "title":
        query = """
        SELECT F.film_id, F.title, 
            GROUP_CONCAT(DISTINCT C.name ORDER BY C.name SEPARATOR ', ') AS genres,
            F.release_year, F.rental_rate, F.description
        FROM film AS F, film_category AS FC, category as C
        WHERE FC.category_id = C.category_id AND
        F.title LIKE %s
        GROUP BY F.film_id
        ORDER BY F.title;
        """
    elif category == "actor":
        query = """
        SELECT F.film_id, F.title,
            GROUP_CONCAT(DISTINCT C.name ORDER BY C.name SEPARATOR ', ') AS genres, 
            F.release_year, F.rental_rate, F.description, A.first_name, A.last_name
        FROM film AS F, film_actor AS FA, actor as A, film_category AS FC, category AS C
        WHERE FA.film_id = F.film_ID AND A.actor_id = FA.actor_id AND C.category_id = FC.category_id AND
        A.first_name LIKE %s OR A.last_name LIKE %s
        GROUP BY F.film_id, A.first_name, A.last_name
        ORDER BY F.title;
        """
    elif category == "genre":
        query = """
        SELECT F.film_id, F.title, 
            GROUP_CONCAT(DISTINCT C.name ORDER BY C.name SEPARATOR ', ') AS genres,
            F.release_year, F.rental_rate, F.description
        FROM film AS F, film_category AS FC, category AS C
        WHERE FC.film_id = F.film_id AND C.category_id = FC.category_id AND
        C.name LIKE %s
        GROUP BY F.film_id
        ORDER BY F.title;
        """
    else:
        return jsonify({"error": "Invalid entry"}), 400
    
    # take text entered and put it into the query
    like_query = f"%{search_query}%"
    if category == "actor":
        cursor.execute(query, (like_query, like_query))
    else:
        cursor.execute(query, (like_query,))

    result = cursor.fetchall()
    cursor.close()

    # populate with film info
    films = []
    for row in result:
        film = {
            "film_id": row[0], 
            "title": row[1], 
            "genre": row[2],
            "release_year": row[3],
            "rental_rate": row[4],
            "description": row[5]}
        if category == "actor":
            actor_full_name = row[6] + " " + row[7]
            film["actor_name"] = actor_full_name
        films.append(film)

    return jsonify(films)

@app.route('/rentfilm', methods=['POST'])
def rent_film():
    data = request.get_json()
    customer_id = data.get("customer_id")
    film_id = data.get("film_id")

    if not customer_id or not film_id:
        return jsonify({"error": "customer_id and film_id are required."}), 400

    try:
        film_id = int(film_id)
        customer_id = int(customer_id)
    except ValueError:
        return jsonify({"error": "Invalid data. customer_id and film_id must be integers."}), 400

    conn = mysql.connection
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT I.inventory_id
    FROM rental AS R, store AS S, inventory AS I, film AS F
    WHERE R.return_date IS NOT NULL AND S.store_id = 1 AND I.store_id = S.store_id AND I.inventory_id = R.inventory_id AND I.film_id = F.film_id AND F.film_id = %s AND S.store_id = 1;;
    """

    try:
        cursor.execute(query, (film_id,))
        available_inventory = cursor.fetchone()

        print(available_inventory)

        if not available_inventory:
            return jsonify({"error": "Film is not available for rent."}), 400

        inventory_id = available_inventory[0]

        print(inventory_id)

        rental_date = datetime.now()
        formatted_rental_date = rental_date.strftime('%Y-%m-%d %H:%M:%S')
        query_rental = """
        INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
        VALUES (%s, %s, %s, NULL, 1);
        """
        cursor.execute(query_rental, (formatted_rental_date, inventory_id, customer_id))
        conn.commit()

        update_query = """
        UPDATE rental
        SET return_date = NULL
        WHERE inventory_id = %s AND return_date IS NOT NULL;
        """
        cursor.execute(update_query, (inventory_id,))
        conn.commit()

        cursor.close()
        return jsonify({"success": "Film rented successfully!"})

    except Exception as e:
        print(f"Database error: {str(e)}")
        return jsonify({"error": "An error occurred while processing the rental."}), 500
    
@app.route('/api/returnRental/<int:rental_id>', methods=['PUT'])
def return_film(rental_id):
    data = request.get_json()

    print(data)
    customer_id = data.get('customer_id')
    film_id = data.get('film_id')

    if not customer_id or not film_id:
        return jsonify({"error": "customer_id and film_id are required."}), 400

    conn = mysql.connection
    cursor = conn.cursor()

    query = """
    SELECT R.rental_id, R.inventory_id
    FROM rental AS R
    JOIN inventory AS I ON R.inventory_id = I.inventory_id
    WHERE R.customer_id = %s AND I.film_id = %s AND R.return_date IS NULL
    LIMIT 1;
    """
    
    cursor.execute(query, (customer_id, film_id))
    rental = cursor.fetchone()

    if not rental:
        return jsonify({"error": "No active rental found for the specified customer and film."}), 404

    # rental_id = rental[0]
    inventory_id = rental[1]
    return_date = datetime.now()

    formatted_return_date = return_date.strftime('%Y-%m-%d %H:%M:%S')
    
    update_query = """
    UPDATE rental
    SET return_date = %s
    WHERE rental_id = %s;
    """
    try:
        cursor.execute(update_query, (formatted_return_date, rental_id))
        conn.commit()
        cursor.close()
        return jsonify({"success": "Film returned successfully!"})
    except Exception as e:
        print(f"Database error: {str(e)}")
        return jsonify({"error": "An error occurred while processing the return."}), 500

@app.route('/displaycustomers', methods=['GET'])
def displaycustomers():
    # Dealing with pages
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    conn = mysql.connection
    cursor = conn.cursor()

    # Query to count total customers
    cursor.execute("SELECT COUNT(*) FROM customer")
    total_customers = cursor.fetchone()[0]
    total_pages = (total_customers + per_page - 1) // per_page

    # Get customers for the current page
    offset = (page - 1) * per_page
    query = """
    SELECT customer_id, first_name, last_name
    FROM customer
    ORDER BY customer_id
    LIMIT %s OFFSET %s;
    """
    cursor.execute(query, (per_page, offset))
    customers = cursor.fetchall()

    cursor.close()

    # Convert list to JSON
    customer_list = [{"customer_id": c[0], "first_name": c[1], "last_name": c[2]} for c in customers]

    return jsonify({"customers": customer_list, "total_pages": total_pages})


@app.route('/searchcustomers', methods=['GET'])
def search_customers():
    search_query = request.args.get("query", "").strip()
    category = request.args.get("category", "customer_id")

    if not search_query:
        return jsonify([])

    conn = mysql.connection
    cursor = conn.cursor()

    # Query on category (customer_id/first_name/last_name)
    if category == "customer_id":
        query = """
        SELECT customer_id, first_name, last_name
        FROM customer
        WHERE customer_id LIKE %s
        ORDER BY customer_id;
        """
    elif category == "first_name":
        query = """
        SELECT customer_id, first_name, last_name
        FROM customer
        WHERE first_name LIKE %s
        ORDER BY first_name;
        """
    elif category == "last_name":
        query = """
        SELECT customer_id, first_name, last_name
        FROM customer
        WHERE last_name LIKE %s
        ORDER BY last_name;
        """
    else:
        return jsonify({"error": "Invalid entry"}), 400

    # Add "%" for LIKE query
    like_query = f"%{search_query}%"
    cursor.execute(query, (like_query,))

    result = cursor.fetchall()
    cursor.close()

    # Return the list of customers
    customers = []
    for row in result:
        customer = {
            "customer_id": row[0],
            "first_name": row[1],
            "last_name": row[2]
        }
        customers.append(customer)

    return jsonify(customers)

@app.route('/addcustomer', methods=['POST'])
def add_customer():
    data = request.get_json()
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    email = data.get("email")

    if not first_name or not last_name or not email:
        return jsonify({"error": "First name, last name, and email are required."}), 400

    conn = mysql.connection
    cursor = conn.cursor()

    insert_customer = """
    INSERT INTO customer (store_id, first_name, last_name, email, address_id)
    VALUES (1, %s, %s, %s, 1);
    """
    cursor.execute(insert_customer, (first_name, last_name, email))
    conn.commit()

    cursor.close()

    return jsonify({"success": "Customer added successfully!"})

@app.route('/updatecustomer/<int:customer_id>', methods=['PUT'])
def update_customer(customer_id):
    data = request.get_json()
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')

    cursor = mysql.connection.cursor()
    cursor.execute("""
        UPDATE customer 
        SET first_name = %s, last_name = %s, email = %s 
        WHERE customer_id = %s
    """, (first_name, last_name, email, customer_id))
    mysql.connection.commit()
    cursor.close()

    return jsonify({"success": "Customer updated successfully, refresh to view your changes."})

@app.route('/deletecustomer/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    conn = mysql.connection
    cursor = conn.cursor()

    # Must delete records in other tables with this customer id due to foreign key constraints
    delete_payment_query = "DELETE FROM payment WHERE customer_id = %s"
    cursor.execute(delete_payment_query, (customer_id,))
    conn.commit()

    delete_rental_query = "DELETE FROM rental WHERE customer_id = %s"
    cursor.execute(delete_rental_query, (customer_id,))
    conn.commit()

    delete_customer_query = "DELETE FROM customer WHERE customer_id = %s"
    cursor.execute(delete_customer_query, (customer_id,))
    conn.commit()

    if cursor.rowcount > 0:
        return jsonify({"success": "Customer and dependent records deleted successfully! Refresh to reflect deletion"})
    else:
        return jsonify({"error": "Customer not found."}), 404

@app.route('/api/customer/<int:customer_id>', methods=['GET'])
def get_customer_details(customer_id):

    conn = mysql.connection
    cursor = conn.cursor()

    query = """
    SELECT C.customer_id, C.first_name, C.last_name, C.email, C.create_date, R.rental_id, F.title, R.return_date, F.film_id
    FROM rental as R, customer as C, inventory as I, film as F
    WHERE C.customer_id = R.customer_id AND I.inventory_id = R.inventory_id AND F.film_id = I.film_id AND C.customer_id = %s
    ORDER BY R.return_date DESC;
    """
    cursor.execute(query, (customer_id,))
    result = cursor.fetchall()
    cursor.close()

    customer_info = {
        'customer_id': result[0][0],
        'first_name': result[0][1],
        'last_name': result[0][2],
        'email': result[0][3],
        'create_date': result[0][4],
        'rentals': []
    }

    # Loop through the result and collect rental information
    for row in result:
        rental = {
            'rental_id': row[5],
            'title': row[6],
            'return_date': row[7],
            'film_id': row[8]
        }
        customer_info['rentals'].append(rental)

    # Return customer details along with rentals
    return jsonify(customer_info)

if __name__ == "__main__":
    app.run(debug = True)
