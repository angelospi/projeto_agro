
CREATE TABLE project_agro.Countries (
	id int64 NOT NULL ,
	name string NOT NULL
 );

ALTER TABLE project_agro.Countries
ADD PRIMARY KEY(id) NOT ENFORCED;

CREATE TABLE project_agro.Elements (
	id string  NOT NULL,
	year int64  NOT NULL,
	value float64  NOT NULL,
	unit string  NOT NULL,
	flag string  NOT NULL,
	description_flag string ,
	code_item int64  NOT NULL,
	code_country int64  NOT NULL,
	name_element string NOT NULL,
 );

ALTER TABLE project_agro.Elements
ADD PRIMARY KEY(id) NOT ENFORCED;

CREATE TABLE project_agro.Item (
	id int64 NOT NULL ,
	name string NOT NULL
 );

ALTER TABLE project_agro.Item
ADD PRIMARY KEY(id) NOT ENFORCED;


CREATE TABLE project_agro.Population (
	id string NOT NULL ,
	id_country int64 NOT NULL ,
	year int64 NOT NULL ,
	value float64 NOT NULL ,
	footnotes string  ,
 );

ALTER TABLE project_agro.Population
ADD PRIMARY KEY(id) NOT ENFORCED;

ALTER TABLE project_agro.Population
ADD CONSTRAINT fk_countries FOREIGN KEY (id_country)
REFERENCES project_agro.Countries(id) NOT ENFORCED;

ALTER TABLE project_agro.Elements
ADD CONSTRAINT fk_item FOREIGN KEY (code_item)
REFERENCES project_agro.Item(id) NOT ENFORCED;

ALTER TABLE project_agro.Elements
ADD CONSTRAINT fk_countries FOREIGN KEY (code_country)
REFERENCES project_agro.Countries(id) NOT ENFORCED;
