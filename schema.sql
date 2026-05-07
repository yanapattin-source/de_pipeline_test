CREATE TABLE dim_department (
    department_id  SERIAL PRIMARY KEY,
    department_name VARCHAR(32) NOT NULL UNIQUE
);

CREATE TABLE dim_sensor (
    sensor_id      SERIAL PRIMARY KEY,
    sensor_serial  VARCHAR(64) NOT NULL,
    department_id  INTEGER NOT NULL REFERENCES dim_department(department_id),
    UNIQUE (sensor_serial, department_id)
);

CREATE TABLE dim_product (
    product_id    SERIAL PRIMARY KEY,
    product_name  VARCHAR(16) NOT NULL UNIQUE
);

CREATE TABLE fact_sensor_reading (
    id             BIGSERIAL PRIMARY KEY,
    sensor_id      INTEGER NOT NULL REFERENCES dim_sensor(sensor_id),
    product_id     INTEGER NOT NULL REFERENCES dim_product(product_id),
    department_id  INTEGER NOT NULL REFERENCES dim_department(department_id),
    create_at      TIMESTAMP NOT NULL,
    product_expire TIMESTAMP NOT NULL
);

-- Performance indexes
CREATE INDEX idx_fact_sensor ON fact_sensor_reading(sensor_id);
CREATE INDEX idx_fact_dept ON fact_sensor_reading(department_id);
CREATE INDEX idx_fact_product ON fact_sensor_reading(product_id);
CREATE INDEX idx_fact_create_at ON fact_sensor_reading(create_at);
