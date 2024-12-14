
-- author
INSERT INTO author (name, last_name, nationality) VALUES ('Flor M.', 'Salvador', 'Mexican');
INSERT INTO author (name, last_name, nationality) VALUES ('Ariana', 'Godoy', 'Venezuelan');
INSERT INTO author (name, last_name, nationality) VALUES ('Sarah J.', 'Maas', 'American');
INSERT INTO author (name, last_name, nationality) VALUES ('Leigh', 'Bardugo', 'American');
INSERT INTO author (name, last_name, nationality) VALUES ('Alex', 'Mírez', 'Mexican');
INSERT INTO author (name, last_name, nationality) VALUES ('Taylor', 'Jenkins', 'American');
INSERT INTO author (name, last_name, nationality) VALUES ('V. E.', 'Schwab', 'American');
INSERT INTO author (name, last_name, nationality) VALUES ('Colleen', 'Hoover', 'American');


-- publisher
INSERT INTO publisher (name) VALUES ('Editorial Norma');
INSERT INTO publisher (name) VALUES ('Penguin Random House');
INSERT INTO publisher (name) VALUES ('Bloomsbury');
INSERT INTO publisher (name) VALUES ('Macmillan');
INSERT INTO publisher (name) VALUES ('Atria');
INSERT INTO publisher (name) VALUES ('Tor');

-- book
INSERT INTO book (title, price, publisher_id, publication_year, stock, language, cover_type) 
VALUES 
('A través de mi ventana', 120.00,  2, 2020, 10, 'Spanish', 'Hardcover'),
('Raza de bronce', 125.00, 2, 2020, 15, 'Spanish', 'Paperback'),
('Boulevard', 122.00, 3, 2021, 12, 'Spanish', 'Hardcover'),
('Cerco de penumbras', 118.00, 4, 2015, 8, 'Spanish', 'Paperback'),
('Perfectos mentirosos', 120.00, 3, 2015, 9, 'Spanish', 'Hardcover'),
('Una corte de rosas y espinas', 119.00, 3, 2021, 7, 'Spanish', 'Paperback'),
('La vida invisible de Addie Larue', 124.00, 3, 2017, 10, 'Spanish', 'Hardcover'),
('Hijo de opa', 21.00, 1, 2020, 11, 'Spanish', 'Paperback'),
('Como agua para chocolate', 23.00, 1, 2020, 13, 'Spanish', 'Hardcover'),
('Romper el círculo', 22.00, 2, 2016, 14, 'Spanish', 'Paperback');

INSERT INTO book_author (book_id, author_id)
VALUES
(1, 5),
(2, 1),
(3, 2),
(4, 3),
(5, 3),
(6, 5),
(7, 8),
(8, 7),
(9, 6),
(10, 4);


-- genre
INSERT INTO genre (name) VALUES ('Fiction');
INSERT INTO genre (name) VALUES ('Non-Fiction');
INSERT INTO genre (name) VALUES ('Fantasy');
INSERT INTO genre (name) VALUES ('Mystery');
INSERT INTO genre (name) VALUES ('Biography');
INSERT INTO genre (name) VALUES ('Romance');
INSERT INTO genre (name) VALUES ('Children');

-- book_genre between Bolivian books y genres
INSERT INTO book_genre (book_id, genre_id) VALUES (1, 6);  
INSERT INTO book_genre (book_id, genre_id) VALUES (2, 6);  
INSERT INTO book_genre (book_id, genre_id) VALUES (3, 5);  
INSERT INTO book_genre (book_id, genre_id) VALUES (4, 1); 
INSERT INTO book_genre (book_id, genre_id) VALUES (5, 6);  
INSERT INTO book_genre (book_id, genre_id) VALUES (6, 4);  
INSERT INTO book_genre (book_id, genre_id) VALUES (7, 6); 
INSERT INTO book_genre (book_id, genre_id) VALUES (8, 1); 
INSERT INTO book_genre (book_id, genre_id) VALUES (9, 1);  
INSERT INTO book_genre (book_id, genre_id) VALUES (10, 3); 

-- customer
INSERT INTO customer (name, last_name, email) VALUES ('Carlos', 'Ruiz', 'carlos.ruiz@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Elena', 'Ferrer', 'elena.ferrer@ucb.edu.bo');
INSERT INTO customer (name, last_name, email) VALUES ('Maria', 'Lopez', 'maria.lopez@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Luis', 'Gomez', 'luis.gomez@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Ana', 'Perez', 'ana.perez@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Juan', 'Ruiz', 'juan.ruiz@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Carla', 'Ferrer', 'carla.ferrer@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Darian', 'Lopez', 'darian.lopez@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Fernando', 'Gomez', 'fernando.gomez@gmail.com');
INSERT INTO customer (name, last_name, email) VALUES ('Lucas', 'Perez', 'lucas.perez@gmail.com');

-- seller
INSERT INTO seller (name, last_name, phone, email) VALUES ('Jose', 'Ramirez', '7901234', 'jose.ramirez@gmail.com');
INSERT INTO seller (name, last_name, phone, email) VALUES ('Lucia', 'Hernandez', '7775678', 'lucia.hernandez@gmail.com');
INSERT INTO seller (name, last_name, phone, email) VALUES ('Pedro', 'Gonzalez', '7098765', 'pedro.gonzales@gmail.com');

-- city
INSERT INTO city (name_city) VALUES ('Santa Cruz');
INSERT INTO city (name_city) VALUES ('La Paz');

-- bill
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (1, 1, 1, '2024-12-01'); -- Compra en Santa Cruz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (2, 2, 1, '2024-12-02'); -- Compra en Santa Cruz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (3, 3, 2, '2024-12-03'); -- Compra en La Paz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (4, 1, 2, '2024-12-04'); -- Compra en La Paz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (5, 2, 1, '2024-12-05'); -- Compra en Santa Cruz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (7, 1, 1, '2024-12-01'); -- Compra en Santa Cruz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (6, 2, 1, '2024-12-02'); -- Compra en Santa Cruz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (8, 3, 2, '2024-12-03'); -- Compra en La Paz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (9, 1, 2, '2024-12-04'); -- Compra en La Paz
INSERT INTO sales_order (customer_id, seller_id, city_id, date) VALUES (10, 2, 1, '2024-12-05'); -- Compra en Santa Cruz

-- Add books to bills
INSERT INTO sales_order_book (sales_order_id, book_id, quantity) VALUES 
(1, 1, 2),  
(1, 2, 1),  
(2, 3, 1), 
(3, 2, 2),  
(4, 2, 1),  
(5, 2, 1), 
(6, 1, 2),  
(7, 4, 1),  
(8, 9, 1),
(9, 3, 2),  
(10, 8, 1),  
(9, 9, 1);   

