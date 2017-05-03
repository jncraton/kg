-- used to store information about where data came from
create table datasources (
  uri text,
  sha256 text,
  accessed datetime default current_timestamp,
  id integer primary key
);

-- factsets represent linked facts
-- e.g. a row from a csv file
create table factsets (
  name text, -- may be blank initially or guessed
  datasource_id integer,
  datasource_field text, -- e.g. row 4
  entity_id integer,
  id integer primary key
);

-- facts are small pieces of information about an entity
-- e.g. cell values from csv file
create table facts (
  value text,
  label text, -- e.g. column name
  factset_id integer,
  id integer primary key
);

create index facts_value_idx on facts(value);

-- an entity is something that exists that has associated facts
-- e.g. a person
create table entities (
  id integer primary key
)