from flask import Flask, request, jsonify
import pyodbc

app = Flask(__name__)

DB_CONFIG = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "(LocalDb)\\MSSQLLocalDB",  # LocalDB server name
    "database": "SpatialData",             # Database name
    "uid": "",                            # Leave empty for Windows Authentication
    "pwd": ""                             # Leave empty for Windows Authentication
}

# Establish DB Connection
def get_db_connection():
    try:
        conn = pyodbc.connect(
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"Trusted_Connection=yes;"  # Using Windows Authentication
        )
        print("Connection successful!")
        return conn
    except Exception as e:
        print("Error while connecting to the database:", e)
        return None
    

@app.route('/points', methods=['POST', 'GET'])
def points():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()

        if request.method == 'POST':
            data = request.json

            # If data is a list (multiple points)
            if isinstance(data, list):
                for point in data:
                    name = point.get('name')
                    lat = point.get('latitude')
                    lon = point.get('longitude')

                    if not name or lat is None or lon is None:
                        return jsonify({"error": "Missing required data for point"}), 400

                    query = "INSERT INTO Points (Name, Location) VALUES (?, geography::Point(?, ?, 4326))"
                    cursor.execute(query, name, lat, lon)
                conn.commit()
                return jsonify({"message": "Points added successfully"}), 201

            # If data is a single point
            elif isinstance(data, dict):
                name = data.get('name')
                lat = data.get('latitude')
                lon = data.get('longitude')

                if not name or lat is None or lon is None:
                    return jsonify({"error": "Missing required data"}), 400

                query = "INSERT INTO Points (Name, Location) VALUES (?, geography::Point(?, ?, 4326))"
                cursor.execute(query, name, lat, lon)
                conn.commit()
                return jsonify({"message": "Point added successfully"}), 201

            else:
                return jsonify({"error": "Invalid data format"}), 400

        elif request.method == 'GET':
            cursor.execute("SELECT Id, Name, Location.ToString() AS Location FROM Points")
            points = cursor.fetchall()
            results = [{"id": row[0], "name": row[1], "location": row[2]} for row in points]
            return jsonify(results)

    except Exception as e:
        print(f"Error while handling request: {e}")
        return jsonify({"error": "An error occurred while processing your request"}), 500

    finally:
        if conn:
            conn.close()

@app.route('/polygons', methods=['POST', 'GET'])
def polygons():
    try:
        # Establish database connection
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()

        # Handle POST request
        if request.method == 'POST':
            data = request.json

            # If data is a list (multiple polygons)
            if isinstance(data, list):
                for polygon in data:
                    name = polygon.get('name')
                    polygon_wkt = polygon.get('polygon_wkt')

                    # Validate incoming data
                    if not name or not polygon_wkt:
                        return jsonify({"error": "Missing required data for polygon"}), 400

                    # Insert polygon into the database
                    query = "INSERT INTO Polygons (Name, Area) VALUES (?, geography::STPolyFromText(?, 4326))"
                    cursor.execute(query, name, polygon_wkt)
                conn.commit()
                return jsonify({"message": "Polygons added successfully"}), 201

            # If data is a single polygon
            elif isinstance(data, dict):
                name = data.get('name')
                polygon_wkt = data.get('polygon_wkt')

                # Validate incoming data
                if not name or not polygon_wkt:
                    return jsonify({"error": "Missing required data for polygon"}), 400

                # Insert polygon into the database
                query = "INSERT INTO Polygons (Name, Area) VALUES (?, geography::STPolyFromText(?, 4326))"
                cursor.execute(query, name, polygon_wkt)
                conn.commit()
                return jsonify({"message": "Polygon added successfully"}), 201

            else:
                return jsonify({"error": "Invalid data format"}), 400

        # Handle GET request
        elif request.method == 'GET':
            cursor.execute("SELECT Id, Name, Area.ToString() AS Area FROM Polygons")
            polygons = cursor.fetchall()
            results = [{"id": row[0], "name": row[1], "area": row[2]} for row in polygons]
            return jsonify(results)

    except Exception as e:
        # Log the error
        print(f"Error while handling request: {e}")
        return jsonify({"error": "An error occurred while processing your request"}), 500

    finally:
        # Close database connection
        if conn:
            conn.close()


@app.route('/check_point_in_polygon', methods=['POST'])
def check_point_in_polygon():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()

        # Get the request data
        data = request.json
        point_lat = data.get('latitude')
        point_lon = data.get('longitude')
        polygon_id = data.get('polygon_id')

        if point_lat is None or point_lon is None or polygon_id is None:
            return jsonify({"error": "Missing required data (latitude, longitude, or polygon_id)"}), 400

        # Query to check if the point is inside the polygon
        query = """
        SELECT CASE
            WHEN Area.STContains(geography::Point(?, ?, 4326)) = 1 THEN 'Inside'
            ELSE 'Outside'
        END AS result
        FROM Polygons
        WHERE Id = ?
        """
        cursor.execute(query, point_lon, point_lat, polygon_id)
        result = cursor.fetchone()

        if result:
            return jsonify({"result": result[0]}), 200
        else:
            return jsonify({"error": "Polygon not found"}), 404

    except Exception as e:
        print(f"Error while checking if point is inside polygon: {e}")
        return jsonify({"error": "An error occurred while processing your request"}), 500

    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    app.run(debug=True , use_reloader=False)
