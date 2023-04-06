-- This query creates a new database named "config"
CREATE DATABASE config; 

-- This query selects the newly created database
USE config; 

-- This query creates a new table named "settings" in the "config" database
CREATE TABLE config (
  name VARCHAR(50) PRIMARY KEY,
  value VARCHAR(50)
); 

-- This query inserts a sample record into the "settings" table
INSERT INTO config (name, value) 
VALUES 
('STOCKER_MYSQL_USER', 'stocker'),
('STOCKER_MYSQL_DB', 'stocker'),
('STOCKER_MYSQL_PASS', '2016@uq$tencent'),
('STOCKER_MYSQL_HOST', 'localhost'),
('EMAIL_USER', '515205935@qq.com'),
('EMAIL_PASS', 'qlgfqeimqrjrbheb'),
('EMAIL_SMTP', 'smtp.qq.com'),
('EMAIL_SMTP_PORT', '465'); 

-- This code block selects all records from the "settings" table to verify the insert was successful
SELECT * FROM config;