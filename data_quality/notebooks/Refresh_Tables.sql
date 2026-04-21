
update [dbo].[counter_file_id]
set counter = 1;

TRUNCATE TABLE [dbo].[failed_queries]
TRUNCATE TABLE [dbo].[successful_queries]
TRUNCATE TABLE [dbo].[complete_conformity_query_output];
TRUNCATE TABLE [dbo].[duplicity_query_output];
TRUNCATE TABLE [dbo].[tbl_errordetails_datamodel];
TRUNCATE TABLE [dbo].[tbl_error_count_by_attribute_datamodel_updated];
TRUNCATE TABLE [dbo].[tbl_universe_datamodel];
TRUNCATE TABLE [dbo].[tbl_country_universe_ref];
TRUNCATE TABLE [dbo].[tbl_error_count_by_attribute_datamodel];
TRUNCATE TABLE [dbo].[tbl_error_completeness_datamodel];
TRUNCATE TABLE [dbo].[tbl_error_conformity_datamodel];
TRUNCATE TABLE [dbo].[tbl_error_duplicity_datamodel];
