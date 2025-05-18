CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

CREATE TABLE IF NOT EXISTS raw_flights (
  airline VARCHAR(255),
  source VARCHAR(50),
  source_name VARCHAR(255),
  destination VARCHAR(50),
  destination_name VARCHAR(255),
  departure_datetime DATETIME,
  arrival_datetime DATETIME,
  duration_hours DECIMAL(10,2),
  stopovers VARCHAR(50),
  aircraft_type VARCHAR(100),
  travel_class VARCHAR(50),
  booking_source VARCHAR(100),
  base_fare_bdt DECIMAL(15,2),
  tax_surcharge_bdt DECIMAL(15,2),
  total_fare_bdt DECIMAL(15,2),
  seasonality VARCHAR(50),
  days_before_departure INT
);

CREATE USER IF NOT EXISTS 'flight_user'@'%' IDENTIFIED BY 'flight_password';
GRANT ALL PRIVILEGES ON flight_staging.* TO 'flight_user'@'%';
FLUSH PRIVILEGES;
