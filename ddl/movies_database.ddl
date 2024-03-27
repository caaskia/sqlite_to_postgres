CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE "content".genre (
	id uuid NOT NULL,
	created timestamptz NOT NULL,
	modified timestamptz NOT NULL,
	"name" varchar(255) NOT NULL,
	description text NULL,
	CONSTRAINT genre_pkey PRIMARY KEY (id)
);


CREATE TABLE "content".person (
	id uuid NOT NULL,
	created timestamptz NOT NULL,
	modified timestamptz NOT NULL,
	full_name text NOT NULL,
	CONSTRAINT person_pkey PRIMARY KEY (id)
);


CREATE TABLE "content".film_work (
	id uuid NOT NULL,
	created timestamptz NOT NULL,
	modified timestamptz NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	creation_date date NULL,
	rating float8 NULL,
	"type" varchar(8) NOT NULL,
	certificate varchar(512) NULL,
	file_path varchar(100) NULL,
	CONSTRAINT film_work_pkey PRIMARY KEY (id)
);


CREATE TABLE "content".genre_film_work (
	id uuid NOT NULL,
	created timestamptz NOT NULL,
	film_work_id uuid NOT NULL,
	genre_id uuid NOT NULL,
	CONSTRAINT genre_film_work_pkey PRIMARY KEY (id)
);
CREATE INDEX genre_film_work_film_work_id_65abe300 ON content.genre_film_work USING btree (film_work_id);
CREATE INDEX genre_film_work_genre_id_88fbcf0d ON content.genre_film_work USING btree (genre_id);

ALTER TABLE "content".genre_film_work ADD CONSTRAINT genre_film_work_film_work_id_65abe300_fk_film_work_id FOREIGN KEY (film_work_id) REFERENCES "content".film_work(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "content".genre_film_work ADD CONSTRAINT genre_film_work_genre_id_88fbcf0d_fk_genre_id FOREIGN KEY (genre_id) REFERENCES "content".genre(id) DEFERRABLE INITIALLY DEFERRED;


CREATE TABLE "content".person_film_work (
	id uuid NOT NULL,
	"role" varchar(8) NOT NULL,
	created timestamptz NOT NULL,
	film_work_id uuid NOT NULL,
	person_id uuid NOT NULL,
	CONSTRAINT person_film_work_pkey PRIMARY KEY (id)
);
CREATE INDEX person_film_work_film_work_id_1724c536 ON content.person_film_work USING btree (film_work_id);
CREATE INDEX person_film_work_person_id_196d24de ON content.person_film_work USING btree (person_id);


ALTER TABLE "content".person_film_work ADD CONSTRAINT person_film_work_film_work_id_1724c536_fk_film_work_id FOREIGN KEY (film_work_id) REFERENCES "content".film_work(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "content".person_film_work ADD CONSTRAINT person_film_work_person_id_196d24de_fk_person_id FOREIGN KEY (person_id) REFERENCES "content".person(id) DEFERRABLE INITIALLY DEFERRED;


