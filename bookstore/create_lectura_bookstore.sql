-- Create database
CREATE DATABASE bookstore;
USE bookstore;

-- Create table author
CREATE TABLE author (
    author_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    nationality VARCHAR(255)
);

-- Create table publisher
CREATE TABLE publisher (
    publisher_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Create table genre
CREATE TABLE genre (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Create table book
CREATE TABLE book (
    book_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    publisher_id INT,
    publication_year YEAR,
    stock INT,
    language VARCHAR(50),
    cover_type VARCHAR(50),
    FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id)
);

-- Create table book_genre
CREATE TABLE book_genre (
	book_genre_id INT AUTO_INCREMENT PRIMARY KEY,
    book_id INT,
    genre_id INT,
    FOREIGN KEY (book_id) REFERENCES book(book_id),
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
);

-- Create table book_author
CREATE TABLE book_author (
	book_author_id INT AUTO_INCREMENT PRIMARY KEY,
    book_id INT,
    author_id INT,
    FOREIGN KEY (book_id) REFERENCES book(book_id),
    FOREIGN KEY (author_id) REFERENCES author(author_id)
);

-- Create table customer
CREATE TABLE customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(55) NOT NULL,
    last_name VARCHAR(55) NOT NULL,
    email VARCHAR(55)
);

-- Create table seller
CREATE TABLE seller (
    seller_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(55) NOT NULL,
    last_name VARCHAR(55) NOT NULL,
    phone VARCHAR(20),
    email VARCHAR(55)
);

-- Create table city 
CREATE TABLE city (
    city_id INT AUTO_INCREMENT PRIMARY KEY,
    name_city VARCHAR(55) NOT NULL
);

-- Create table bill
CREATE TABLE sales_order (
    sales_order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    seller_id INT,
    city_id INT,
    date DATE,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
    FOREIGN KEY (city_id) REFERENCES city(city_id)
);

-- Create table bill_book 
CREATE TABLE sales_order_book (
	sales_order_book_id INT AUTO_INCREMENT PRIMARY KEY,
    sales_order_id INT,
    book_id INT,
    quantity INT,
    FOREIGN KEY (sales_order_id) REFERENCES sales_order(sales_order_id),
    FOREIGN KEY (book_id) REFERENCES book(book_id)
);
