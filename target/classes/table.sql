drop table if exists users;

create table users (
	id int not null auto_increment,
	name varchar(255),
	primary key(id)
);

insert into users (name) values('a1');
insert into users (name) values('a2');
insert into users (name) values('a3');
insert into users (name) values('a4');
insert into users (name) values('a5');
insert into users (name) values('a6');
insert into users (name) values('a7');
insert into users (name) values('a8');
insert into users (name) values('a9');
insert into users (name) values('a10');