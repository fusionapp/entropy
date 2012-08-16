-- This can be created using the cqlsh command line interface as follows:
-- cqlsh --cql3 < shannon-1.cql

USE shannon;

CREATE TABLE shannon (
    shannonID uuid,
    created timestamp,
    description text,
    PRIMARY KEY (shannonID));


CREATE TABLE attachments (
    shannonID uuid,
    name text,
    entropyID text,
    PRIMARY KEY (shannonID, name));


CREATE TABLE tags (
    shannonID uuid,
    key text,
    value text,
    PRIMARY KEY (shannonID, key));
