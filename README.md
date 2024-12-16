# SpatialDataAPI
Spatial data platform where we are writing the backend APIs to perform different spatial Queries and storing spatial datasets. Using Python to design and develop the REST API  to achieve the features.

# Spatial Data API

This project provides a REST API for managing spatial data (points and polygons). It includes functionality for storing, updating, retrieving, and performing spatial queries like checking if a point is inside a polygon.

## Features
- Add and retrieve multiple points or polygons.

## Prerequisites
1. Python 3.x installed on your system.
2. SQL Server installed locally or on a server.
3. Required Python packages: `Flask`, `pyodbc`.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/<repo-name>.git
   cd SpatialDataAPI

Endpoints
POST /points - Add a single point or multiple points.
GET /points - Retrieve all points.
POST /polygons - Add a single polygon or multiple polygons.
GET /polygons - Retrieve all polygons.

Example Data:
  [
  {
  "name": "Paris",
  "latitude": 48.8584,
  "longitude": 2.2945
}
]

{
  "name": "France",
  "polygon_wkt": "POLYGON((2.2634 48.8156, 2.4697 48.8156, 2.4697 48.9021, 2.2634 48.9021, 2.2634 48.8156))"
}

