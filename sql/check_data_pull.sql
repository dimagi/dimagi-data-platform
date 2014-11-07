select name, (attributes -> '# Form Submissions')::int
from domain where id not in 
(select domain_id from hq_extract_log where extractor ilike 'CommCareExportDeviceLogExtractor') and active = true
order by (attributes -> '# Form Submissions')::int desc

-- done: 141 153 202
-- not done: 278 254 217

select sum((attributes -> '# Form Submissions')::int)
from domain where id in 
(select domain_id from hq_extract_log where extractor ilike 'CommCareExportDeviceLogExtractor') and active = true

-- done: 4 224 868 468 9210
-- not done: 1 721 759 171085 1 257 417

select count (id) from device_log where domain_id = (select id from domain where name like 'tulasalud')