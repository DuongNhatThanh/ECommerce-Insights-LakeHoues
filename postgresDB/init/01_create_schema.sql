-- =====================================================================
-- Khởi tạo schema "ecommerce" cho database "ecom"
-- Mỗi bảng có thêm cột BIGSERIAL "id" làm khóa chính tăng dần,
-- vì DAG Import_Ecom_data_to_Bronze_State_using_Spark_JDBC dùng
-- --incremental-column id (xem spark/apps/jdbc/postgres_to_bronze.py).
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ecommerce;

SET search_path TO ecommerce;

-- Xóa nếu tồn tại (đảm bảo init lại sạch)
DROP TABLE IF EXISTS ecommerce.order_reviews CASCADE;
DROP TABLE IF EXISTS ecommerce.order_payments CASCADE;
DROP TABLE IF EXISTS ecommerce.order_items CASCADE;
DROP TABLE IF EXISTS ecommerce.orders CASCADE;
DROP TABLE IF EXISTS ecommerce.products CASCADE;
DROP TABLE IF EXISTS ecommerce.sellers CASCADE;
DROP TABLE IF EXISTS ecommerce.customers CASCADE;
DROP TABLE IF EXISTS ecommerce.geolocation CASCADE;
DROP TABLE IF EXISTS ecommerce.product_category_name_translations CASCADE;

-- ---------------------------------------------------------------------
-- customers
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.customers (
    id                          BIGSERIAL PRIMARY KEY,
    customer_id                 VARCHAR(64),
    customer_unique_id          VARCHAR(64),
    customer_zip_code_prefix    VARCHAR(16),
    customer_city               VARCHAR(255),
    customer_state              VARCHAR(8)
);

-- ---------------------------------------------------------------------
-- geolocation
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.geolocation (
    id                          BIGSERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(16),
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            VARCHAR(255),
    geolocation_state           VARCHAR(8)
);

-- ---------------------------------------------------------------------
-- sellers
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.sellers (
    id                          BIGSERIAL PRIMARY KEY,
    seller_id                   VARCHAR(64),
    seller_zip_code_prefix      VARCHAR(16),
    seller_city                 VARCHAR(255),
    seller_state                VARCHAR(8)
);

-- ---------------------------------------------------------------------
-- product_category_name_translations
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.product_category_name_translations (
    id                              BIGSERIAL PRIMARY KEY,
    product_category_name           VARCHAR(255),
    product_category_name_english   VARCHAR(255)
);

-- ---------------------------------------------------------------------
-- products
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.products (
    id                          BIGSERIAL PRIMARY KEY,
    product_id                  VARCHAR(64),
    product_category_name       VARCHAR(255),
    product_name_lenght         INTEGER,
    product_description_lenght  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            INTEGER,
    product_length_cm           INTEGER,
    product_height_cm           INTEGER,
    product_width_cm            INTEGER
);

-- ---------------------------------------------------------------------
-- orders
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.orders (
    id                              BIGSERIAL PRIMARY KEY,
    order_id                        VARCHAR(64),
    customer_id                     VARCHAR(64),
    order_status                    VARCHAR(32),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

-- ---------------------------------------------------------------------
-- order_items
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.order_items (
    id                  BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR(64),
    order_item_id       INTEGER,
    product_id          VARCHAR(64),
    seller_id           VARCHAR(64),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(12,2),
    freight_value       NUMERIC(12,2)
);

-- ---------------------------------------------------------------------
-- order_payments
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.order_payments (
    id                      BIGSERIAL PRIMARY KEY,
    order_id                VARCHAR(64),
    payment_sequential      INTEGER,
    payment_type            VARCHAR(32),
    payment_installments    INTEGER,
    payment_value           NUMERIC(12,2)
);

-- ---------------------------------------------------------------------
-- order_reviews
-- ---------------------------------------------------------------------
CREATE TABLE ecommerce.order_reviews (
    id                      BIGSERIAL PRIMARY KEY,
    review_id               VARCHAR(64),
    order_id                VARCHAR(64),
    review_score            INTEGER,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- Index trên cột id (đã là PK nhưng tạo thêm để đảm bảo plan tốt cho
-- truy vấn WHERE id > last_value của Spark JDBC)
CREATE INDEX IF NOT EXISTS idx_customers_id   ON ecommerce.customers(id);
CREATE INDEX IF NOT EXISTS idx_geolocation_id ON ecommerce.geolocation(id);
CREATE INDEX IF NOT EXISTS idx_sellers_id     ON ecommerce.sellers(id);
CREATE INDEX IF NOT EXISTS idx_pcnt_id        ON ecommerce.product_category_name_translations(id);
CREATE INDEX IF NOT EXISTS idx_products_id    ON ecommerce.products(id);
CREATE INDEX IF NOT EXISTS idx_orders_id      ON ecommerce.orders(id);
CREATE INDEX IF NOT EXISTS idx_order_items_id    ON ecommerce.order_items(id);
CREATE INDEX IF NOT EXISTS idx_order_payments_id ON ecommerce.order_payments(id);
CREATE INDEX IF NOT EXISTS idx_order_reviews_id  ON ecommerce.order_reviews(id);
