CREATE TABLE Country
(
  name_country VARCHAR(200) NOT NULL,
  latitude_country FLOAT NOT NULL,
  longtitude_country FLOAT NOT NULL,
  PRIMARY KEY (name_country)
);

CREATE TABLE Organization
(
  name_org VARCHAR(200) NOT NULL,
  category_org VARCHAR(200) NOT NULL,
  PRIMARY KEY (name_org)
);

CREATE TABLE Billionaire
(
  firstname VARCHAR(200) NOT NULL,
  lastname VARCHAR(200) NOT NULL,
  gender CHAR,
  age INT NOT NULL,
  name_country VARCHAR(200) NOT NULL,
  name_org VARCHAR(200) NOT NULL,
  PRIMARY KEY (lastname),
  FOREIGN KEY (name_country) REFERENCES Country(name_country),
  FOREIGN KEY (name_org) REFERENCES Organization(name_org)
);