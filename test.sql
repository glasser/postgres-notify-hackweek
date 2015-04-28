DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name text
);
DROP TABLE IF EXISTS posts CASCADE;
CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  author INTEGER REFERENCES users,
  contents TEXT
);

INSERT INTO users (name) VALUES
 ('David'), ('Justin'), ('Slava'), ('Ekate');

INSERT INTO posts (author, contents) VALUES
 (1, 'glasser post 1'),
 (2, 'justinsb post 1'),
 (1, 'glasser post 2'),
 (4, 'ekate post 1');


/*

SELECT * from users ORDER BY id;

SELECT * from posts ORDER BY id;

SELECT posts.id AS postId, users.name AS userName, posts.contents FROM posts JOIN users ON posts.author = users.id;

SELECT tgname, relname FROM pg_trigger JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid WHERE tgName LIKE 'observe_%';

INSERT INTO posts (author, contents) VALUES (4, 'ekate post 2');

UPDATE users SET name = 'Dave' WHERE name = 'David';

UPDATE users SET name = 'Slava Kim' WHERE name = 'Slava';

SELECT * from users ORDER BY id;

node pgobserve.js 'SELECT posts.id AS postId, users.name AS userName, posts.contents FROM posts JOIN users ON posts.author = users.id' posts users

*/
