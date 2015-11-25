begin transaction;

drop table if exists items;
drop sequence if exists items_seq;
drop table if exists all_types;
drop table if exists extended_types;
drop table if exists geometry_types;
drop table if exists persons;
drop table if exists my_arrays;
drop table if exists numerals;
drop table if exists network_types;
drop table if exists json_and_xml;
drop type if exists person;
drop type if exists address;
drop table if exists my_moods;
drop type if exists days_of_week;
drop type if exists moods;

create sequence items_seq;

create table items (
       items_id INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('items_seq'),
       id int,
       description varchar(200)
);

create table all_types (
       id serial,
       my_boolean boolean,
       my_smallint smallint,
       my_int int,
       my_long bigint,
       my_decimal decimal(19,4),
       my_numeric numeric(10,2),
       my_real real,
       my_double double precision,
       my_money money,
       my_varchar varchar(250),
       my_text text,
       my_bytes bytea,
       my_time time without time zone,
       my_time_tz time with time zone,
       my_date date,
       my_timestamp timestamp without time zone,
       my_timestamp_tz timestamp with time zone
);

insert into items (id, description) values (1, 'one');
insert into items (id, description) values (2, 'two');

insert into all_types (my_boolean, my_smallint, my_int, my_long, my_decimal,
       my_numeric, my_real, my_double, my_money, my_varchar, my_text,
       my_bytes, my_time, my_time_tz, my_date, my_timestamp, my_timestamp_tz)
values (true, 100, 1000, 10000, 175.1234, 1234567.89, 700.5, 700000.1, 250.67, 'some varchar', 'some text',
       E'\\xDEADBEEF', '22:16:52.048607', '22:16:52.048607-05', '2010-12-25',
       '2015-07-07 22:17:38.475474', '2015-07-07 22:17:38.475474-05');

create table extended_types (
       my_bits bit varying(128),
       my_uuid uuid
);

insert into extended_types (my_bits, my_uuid)
values ('110011', 'aa81b166-c60f-4e4e-addb-17414a652733');

create table geometry_types (
       id serial,
       my_point point,
       my_line line,
       my_lseg lseg,
       my_box box,
       my_closed_path path,
       my_open_path path,
       my_polygon polygon,
       my_circle circle
);

insert into geometry_types (my_point, my_line, my_lseg, my_box, my_closed_path, my_open_path, my_polygon, my_circle)
values ('(1,1)', '{1,2,3}', '((1,2),(3,4))', '((3,4),(1,2))', '((0,0),(1,1),(1,0))',
        '[(0,0),(1,1),(1,0)]', '((0,0),(1,1),(1,0))', '<(1,1),5>');

create type address as (
       street1 varchar(100),
       street2 varchar(100),
       city varchar(50),
       postal_code varchar(10),
       lat_long point
);

create type person as (
       birthdate date,
       first_name varchar(100),
       last_name varchar(100),
       favorite_number int,
       stupid_quotes varchar(50),
       the_address address,
       nothing smallint
);

create table persons (
       id serial,
       the_person person
);

insert into persons (the_person) values ('(11-01-1975,"David","Clark",23,"quote""","(""123 Main"""""",""Suite 100"",Fargo,90210,""(45,45)"")",)');

create table my_arrays (
       id serial,
       int_array int[],
       string_array varchar[][]
);

insert into my_arrays (int_array, string_array) values
('{{{1,2,3},{4,5,6},{7,8,9}},{{11,12,13},{14,15,16},{17,18,19}},{{21,22,23},{24,25,26},{27,28,29}}}',
'{{foo,bar},{baz,fuzz}}');

create table numerals (
       id serial,
       arabic int,
       roman varchar(10)
);

insert into numerals (arabic, roman) values(1, 'i');
insert into numerals (arabic, roman) values(2, 'ii');
insert into numerals (arabic, roman) values(3, 'iii');
insert into numerals (arabic, roman) values(4, 'iv');
insert into numerals (arabic, roman) values(5, 'v');
insert into numerals (arabic, roman) values(6, 'vi');
insert into numerals (arabic, roman) values(7, 'vii');
insert into numerals (arabic, roman) values(8, 'viii');
insert into numerals (arabic, roman) values(9, 'ix');
insert into numerals (arabic, roman) values(10, 'x');
insert into numerals (arabic, roman) values(11, 'xi');
insert into numerals (arabic, roman) values(12, 'xii');
insert into numerals (arabic, roman) values(13, 'xiii');
insert into numerals (arabic, roman) values(14, 'xiv');
insert into numerals (arabic, roman) values(15, 'xv');
insert into numerals (arabic, roman) values(16, 'xvi');
insert into numerals (arabic, roman) values(17, 'xvii');
insert into numerals (arabic, roman) values(18, 'xviii');
insert into numerals (arabic, roman) values(19, 'xix');
insert into numerals (arabic, roman) values(20, 'xx');

create table network_types (
       id serial,
       my_macaddr macaddr,
       my_inet inet,
       my_cidr cidr
);

insert into network_types (my_macaddr, my_inet, my_cidr) values ('08:00:2b:01:02:03', '10.10.23.1/32', '192.168.10.0/24');
insert into network_types (my_macaddr, my_inet, my_cidr) values ('08:00:2b:01:02:03', '10.10.23.0/24', '192.168.10.0/24');
insert into network_types (my_macaddr, my_inet, my_cidr) values
('08:00:2b:01:02:17', '2001:4f8:3:ba:2e0:81ff:fe22:0/112', '2001:4f8:3:ba::/64');

create table json_and_xml (
       id serial,
       my_xml xml,
       my_json json,
       my_json_b jsonb
);

insert into json_and_xml (my_xml, my_json, my_json_b)
values
('<?xml version="1.0"?><book><title>Manual</title></book>',
 '{"number": 1, "str": "some string", "array": [ 1, 2, 3, 4, 5 ]}',
 '{"number": 2, "str": "another string", "array": [ 6, 7, 8, 9, 10 ]}');

create or replace function select_numerals() returns refcursor as '
declare
    ref refcursor;
begin
    open ref for select * from numerals;
    return ref;
end;
' language plpgsql;

create or replace function multiple_cursors() returns setof refcursor as '
declare
    one refcursor;
    two refcursor;
    three refcursor;
begin
    open one for select * from numerals;
    return next one;

    open two for select * from items;
    return next two;

    open three for select * from all_types;
    return next three;
end;
' language plpgsql;

create type days_of_week as enum (
       'SUNDAY', 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY'
);

create type moods as enum (
       'HAPPY', 'SAD', 'MAD', 'AFRAID'
);

create table my_moods (
       id serial,
       my_day_of_the_week days_of_week,
       my_mood moods
);

insert into my_moods (my_day_of_the_week, my_mood) values ('MONDAY', 'MAD');
insert into my_moods (my_day_of_the_week, my_mood) values ('FRIDAY', 'HAPPY');

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public to noauth;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public to clearauth;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public to md5auth;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO noauth;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO clearauth;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO md5auth;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public to noauth;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public to clearauth;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public to md5auth;

commit;

-- select oid, typname, typarray from pg_type order by oid asc;
-- select oid, typname, typarray, typrelid from pg_type order by oid asc;
-- select * from pg_attribute where atttypid = 600;

