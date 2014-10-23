use process;
insert overwrite table miloancard_inc 
select * from (
	select o.* from miloancard o left join miloancard_inc n on o.busicode=n.busicode 
	where n.busicode is null 
	union all 
	select * from miloancard_inc
) a;

