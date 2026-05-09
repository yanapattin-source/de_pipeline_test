CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE warehouse.dim_department (
    department_id  SERIAL PRIMARY KEY,
    department_name VARCHAR(32) NOT NULL UNIQUE
);

CREATE TABLE warehouse.dim_sensor (
    sensor_id      SERIAL PRIMARY KEY,
    sensor_serial  VARCHAR(64) NOT NULL,
    department_id  INTEGER NOT NULL,
    UNIQUE (sensor_serial, department_id)
);

CREATE TABLE warehouse.dim_product (
    product_id    SERIAL PRIMARY KEY,
    product_name  VARCHAR(16) NOT NULL UNIQUE
);

CREATE TABLE warehouse.fact_sensor_reading (
    id             BIGSERIAL PRIMARY KEY,
    sensor_id      INTEGER NOT NULL,
    product_id     INTEGER NOT NULL,
    department_id  INTEGER NOT NULL,
    create_at      TIMESTAMP NOT NULL,
    product_expire TIMESTAMP NOT NULL
);

-- Foreign keys
ALTER TABLE warehouse.dim_sensor
    ADD CONSTRAINT fk_sensor_dept FOREIGN KEY (department_id)
        REFERENCES warehouse.dim_department(department_id);

ALTER TABLE warehouse.fact_sensor_reading
    ADD CONSTRAINT fk_fact_sensor FOREIGN KEY (sensor_id)
        REFERENCES warehouse.dim_sensor(sensor_id),
    ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_id)
        REFERENCES warehouse.dim_product(product_id),
    ADD CONSTRAINT fk_fact_dept FOREIGN KEY (department_id)
        REFERENCES warehouse.dim_department(department_id);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_sensor ON warehouse.fact_sensor_reading(sensor_id);
CREATE INDEX IF NOT EXISTS idx_fact_dept ON warehouse.fact_sensor_reading(department_id);
CREATE INDEX IF NOT EXISTS idx_fact_product ON warehouse.fact_sensor_reading(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_create_at ON warehouse.fact_sensor_reading(create_at);
