-- PhEDEx ORACLE schema for agent operations.
-- REQUIRES: OracleCoreTopo.sql, OracleCoreFiles.sql

----------------------------------------------------------------------
-- Create new tables

-- FIXME: partitioning by to_node?
-- FIXME: index organised?
-- FIXME: indexing?

create table t_transfer_tracking
  (timestamp		float		not null,
   guid			char (36)	not null,
   to_node		varchar (20)	not null,
   to_old_state		integer,
   to_new_state		integer		not null,
   from_node		varchar (20)	not null,
   from_old_state	integer,
   from_new_state	integer		not null);

create table t_transfer_history
  (timestamp		float		not null,
   guid			char (36)	not null,
   to_node		varchar (20)	not null,
   to_old_state		integer,
   to_new_state		integer		not null,
   from_node		varchar (20)	not null,
   from_old_state	integer,
   from_new_state	integer		not null);

create table t_transfer_summary
  (timestamp		float		not null,
   guid			char (36)	not null,
   from_node		varchar (20)	not null,
   to_node		varchar (20)	not null,
   assigned		float		not null,
   wanted1st		float,
   wanted		float,
   exported		float,
   started		float,
   completed		float,
   cleared		float,
   errors		integer		not null,
   error_total		float		not null,
   error_began		float);

create table t_perf_histogram
  (timebin		float		not null,
   from_node		varchar (20)	not null,
   to_node		varchar (20)	not null,
   xferred_files	integer,
   xferred_bytes	integer,
   pending_files	integer,
   pending_bytes	integer);

create table t_quality_histogram
  (timebin		float		not null,
   from_node		varchar (20)	not null,
   to_node		varchar (20)	not null,
   old_state		integer		not null,
   new_state		integer		not null,
   transitions		integer		not null);

----------------------------------------------------------------------
-- Add constraints

alter table t_perf_histogram
  add constraint pk_perf_histogram
  primary key (timebin, from_node, to_node)
  using index tablespace CMS_TRANSFERMGMT_INDX01;

alter table t_quality_histogram
  add constraint pk_quality_histogram
  primary key (timebin, from_node, to_node, old_state, new_state)
  using index tablespace CMS_TRANSFERMGMT_INDX01;

----------------------------------------------------------------------
-- Add indices

create index ix_transfer_tracking
  on t_transfer_tracking (timestamp, guid)
  tablespace CMS_TRANSFERMGMT_INDX01;

create index ix_transfer_history
  on t_transfer_history (timestamp, guid)
  tablespace CMS_TRANSFERMGMT_INDX01;

create index ix_transfer_summary
  on t_transfer_summary (timestamp, guid)
  tablespace CMS_TRANSFERMGMT_INDX01;

----------------------------------------------------------------------
-- Modify storage options

alter table t_transfer_tracking			enable row movement;
alter table t_transfer_tracking			move initrans 8;
alter index ix_transfer_tracking		rebuild initrans 8;

alter table t_transfer_history			enable row movement;
alter table t_transfer_history			move initrans 8;
alter index ix_transfer_history			rebuild initrans 8;
