-- ═══════════════════════════════════════════════════════════════════════════
-- MEDIA INDUSTRY DATABASE
-- Streaming Platforms, Content, Users & Analytics
-- ═══════════════════════════════════════════════════════════════════════════

-- Drop tables if they exist (for clean reruns)
DROP TABLE IF EXISTS viewing_history;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS content_catalog;
DROP TABLE IF EXISTS streaming_users;
DROP TABLE IF EXISTS content_creators;
DROP TABLE IF EXISTS departments;

-- ═══════════════════════════════════════════════════════════════════════════
-- TABLE CREATION
-- ═══════════════════════════════════════════════════════════════════════════

-- Departments (Production, Marketing, Tech, etc.)
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100),
    budget DECIMAL(12, 2)
);

-- Content Creators (like employees table)
CREATE TABLE content_creators (
    creator_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2),
    manager_id INT,
    hire_date DATE,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Streaming Users (like customers table)
CREATE TABLE streaming_users (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(50),
    signup_date DATE
);

-- Subscriptions (like orders table)
CREATE TABLE subscriptions (
    subscription_id INT PRIMARY KEY,
    user_id INT,
    plan_type VARCHAR(50),
    total_amount DECIMAL(8, 2),
    subscription_date DATE,
    status VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES streaming_users(user_id)
);

-- Content Catalog (like products table)
CREATE TABLE content_catalog (
    content_id INT PRIMARY KEY,
    title VARCHAR(200),
    content_type VARCHAR(50),
    genre VARCHAR(50),
    release_year INT,
    duration_minutes INT,
    rating DECIMAL(3, 1),
    department_id INT,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Viewing History
CREATE TABLE viewing_history (
    view_id INT PRIMARY KEY,
    user_id INT,
    content_id INT,
    watch_date DATE,
    minutes_watched INT,
    completed BOOLEAN,
    FOREIGN KEY (user_id) REFERENCES streaming_users(user_id),
    FOREIGN KEY (content_id) REFERENCES content_catalog(content_id)
);
