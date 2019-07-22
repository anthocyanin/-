create table course(
cid int(3),
cname varchar(20),
tid int(3)
);
create table teacher(
tid int(3),
tname varchar(20),
tcid int(3)
);
create table teacherCard(
tcid int(3),
tcdesc varchar(200)
);

insert into course values 
	(1, 'java', 1), 
	(2, 'html', 1), 
	(3, 'sql', 2), 
	(4, 'web', 3);

insert into teacher values (1, 'tz', 1), (2, 'tw', 2), (3, 'tl', 3);
insert into teacherCard values (1, 'tzdesc'), (2, 'twdesc'), (3, 'tldesc');


select t.* from teacher t, course c, teacherCard tc where t.tid = c.tid and t.tcid = tc.tcid and (c.cid=2 or tc.tcid=3);

select tc.tcdesc from teacherCard tc, course c, teacher t where c.tid = t.tid and t.tcid = tc.tcid and (c.name='sql');


