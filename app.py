from flask import Flask, request, jsonify , redirect
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
    
    #4326 identifies the WGS 84 coordinate system (latitude and longitude on Earth).
    # Redirect root URL to /points
@app.route('/')
def redirect_to_points():
    return redirect('/points')


@app.route('/points', methods=['POST', 'GET', 'PUT'])
def points():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()

        if request.method == 'POST':
            data = request.json

            if isinstance(data, list):  # Multiple points
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

            elif isinstance(data, dict):  # Single point
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

        elif request.method == 'PUT':
            data = request.json
            point_id = data.get('id')
            name = data.get('name')
            lat = data.get('latitude')
            lon = data.get('longitude')

            if not point_id or not name or lat is None or lon is None:
                return jsonify({"error": "Missing required data"}), 400

            query = """
            UPDATE Points
            SET Name = ?, Location = geography::Point(?, ?, 4326)
            WHERE Id = ?
            """
            cursor.execute(query, name, lat, lon, point_id)
            conn.commit()

            if cursor.rowcount == 0:
                return jsonify({"error": "Point not found"}), 404

            return jsonify({"message": "Point updated successfully"}), 200

    except Exception as e:
        print(f"Error while handling request: {e}")
        return jsonify({"error": "An error occurred while processing your request"}), 500

    finally:
        if conn:
            conn.close()


@app.route('/polygons', methods=['POST', 'GET', 'PUT'])
def polygons():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()

        if request.method == 'POST':
            data = request.json

            if isinstance(data, list):  # Multiple polygons
                for polygon in data:
                    name = polygon.get('name')
                    polygon_wkt = polygon.get('polygon_wkt')

                    if not name or not polygon_wkt:
                        return jsonify({"error": "Missing required data for polygon"}), 400

                    query = "INSERT INTO Polygons (Name, Area) VALUES (?, geography::STPolyFromText(?, 4326))"
                    cursor.execute(query, name, polygon_wkt)
                conn.commit()
                return jsonify({"message": "Polygons added successfully"}), 201

            elif isinstance(data, dict):  # Single polygon
                name = data.get('name')
                polygon_wkt = data.get('polygon_wkt')

                if not name or not polygon_wkt:
                    return jsonify({"error": "Missing required data for polygon"}), 400

                query = "INSERT INTO Polygons (Name, Area) VALUES (?, geography::STPolyFromText(?, 4326))"
                cursor.execute(query, name, polygon_wkt)
                conn.commit()
                return jsonify({"message": "Polygon added successfully"}), 201

            else:
                return jsonify({"error": "Invalid data format"}), 400

        elif request.method == 'GET':
            cursor.execute("SELECT Id, Name, Area.ToString() AS Area FROM Polygons")
            polygons = cursor.fetchall()
            results = [{"id": row[0], "name": row[1], "area": row[2]} for row in polygons]
            return jsonify(results)

        elif request.method == 'PUT':
            data = request.json
            polygon_id = data.get('id')
            name = data.get('name')
            polygon_wkt = data.get('polygon_wkt')

            if not polygon_id or not name or not polygon_wkt:
                return jsonify({"error": "Missing required data"}), 400

            query = """
            UPDATE Polygons
            SET Name = ?, Area = geography::STPolyFromText(?, 4326)
            WHERE Id = ?
            """
            cursor.execute(query, name, polygon_wkt, polygon_id)
            conn.commit()

            if cursor.rowcount == 0:
                return jsonify({"error": "Polygon not found"}), 404

            return jsonify({"message": "Polygon updated successfully"}), 200

    except Exception as e:
        print(f"Error while handling request: {e}")
        return jsonify({"error": "An error occurred while processing your request"}), 500

    finally:
        if conn:
            conn.close()



if __name__ == '__main__':
    app.run(debug=True , use_reloader=False)
