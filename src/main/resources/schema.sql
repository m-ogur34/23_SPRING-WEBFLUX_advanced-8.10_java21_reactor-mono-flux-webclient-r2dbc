CREATE TABLE IF NOT EXISTS products (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    category   VARCHAR(100),
    price      DECIMAL(12,2),
    stock      INT DEFAULT 0,
    active     BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
