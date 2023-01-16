
CREATE TABLE project_agro.Countries (
	id int64 NOT NULL ,
	name string NOT NULL
 );

CREATE TABLE project_agro.Elements (
	id int64  NOT NULL,
	year int64  NOT NULL,
	value float64  NOT NULL,
	unit string  NOT NULL,
	flag string  NOT NULL,
	description_flag string ,
	code_item int64  NOT NULL,
	code_country int64  NOT NULL,
	name_element string NOT NULL,
 );

CREATE TABLE project_agro.Item (
	id int64 NOT NULL ,
	name string NOT NULL
 );

CREATE TABLE project_agro.Population (
	id int64 NOT NULL ,
	id_country int64 NOT NULL ,
	year int64 NOT NULL ,
	Info string  ,
	value float64 NOT NULL ,
	footnotes string  ,
	source string
 );