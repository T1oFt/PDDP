-----v1-----

drop table if exists tdf_finishers;

create table tdf_finishers
with (appendonly=true)
as select * from ext_tdf_finishers
distributed by ("Year", "Team");

drop table if exists tdf_stages;

create table tdf_stages
with (appendonly=true)
as select * from ext_tdf_stages
distributed by ("Year", "Type");

drop table if exists tdf_tours;

create table tdf_tours
with (appendonly=true)
as select * from ext_tdf_tours
distributed by ("Year", "Stages");

drop table if exists tdf_winners;

create table tdf_winners
with (appendonly=true)
as select * from ext_tdf_winners
distributed by ("Year", "Country", "Team");


-----v2-----

drop table if exists tdf_finishers_v2;

create table tdf_finishers_v2
with (appendonly=true)
as select * from ext_tdf_finishers
distributed by ("Year");

drop table if exists tdf_stages_v2;

create table tdf_stages_v2
with (appendonly=true)
as select * from ext_tdf_stages
distributed by ("Year");

drop table if exists tdf_tours_v2;

create table tdf_tours_v2
with (appendonly=true)
as select * from ext_tdf_tours
distributed by ("Year");

drop table if exists tdf_winners_v2;

create table tdf_winners_v2
with (appendonly=true)
as select * from ext_tdf_winners
distributed by ("Year");