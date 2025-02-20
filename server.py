from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_mysqldb import MySQL
import MySQLdb.cursors
from flask_cors import CORS

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

# new query for now (change for MS3), feature 7
@app.route('/displaycustomers', methods=['GET'])
def displaycustomers():

    # dealing with pages
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    conn = mysql.connection
    cursor = conn.cursor()

    # query
    cursor.execute("SELECT COUNT(*) FROM customer")

    # figuring out pages (later try to figure out if there's a way to set the amt of results per page)
    total_customers = cursor.fetchone()[0]
    total_pages = (total_customers + per_page - 1) // per_page

    # get customers for the current page
    offset = (page - 1) * per_page
    query = """
    SELECT first_name, last_name
    FROM customer
    LIMIT %s OFFSET %s;
    """

    # send page query
    cursor.execute(query, (per_page, offset))
    customers = cursor.fetchall()

    cursor.close()

    # convert list to json then send
    customer_list = [{"first_name": c[0], "last_name": c[1]} for c in customers]
    return jsonify({"customers": customer_list, "total_pages": total_pages})

if __name__ == "__main__":
    app.run(debug = True)