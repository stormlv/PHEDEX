package PHEDEX::File::Download::Agent;

use strict;
use warnings;

use base 'PHEDEX::Core::Agent', 'PHEDEX::Core::Logging';
use PHEDEX::Core::JobManager;
use PHEDEX::Core::Catalogue;
use PHEDEX::Core::Command;
use PHEDEX::Core::Timing;
use PHEDEX::Core::DB;
use PHEDEX::Error::Constants;

use List::Util qw(min max sum);
use Data::Dumper;
use POSIX;
use POE;

sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my %params = (DBCONFIG => undef,		# Database configuration file
		  NODES => undef,		# Nodes to operate for
	  	  IGNORE_NODES => [],		# TMDB nodes to ignore
	  	  ACCEPT_NODES => [],		# TMDB nodes to accept

		  VALIDATE_COMMAND => undef,	# pre/post download validation command
		  DELETE_COMMAND => undef,	# pre/post download deletion command
		  PREVALIDATE => 1,             # flag to prevalidate files with VALIDATE_COMMAND
		  PREDELETE => 1,               # flag to predelete files with DELETE_COMMAND

		  TIMEOUT => 600,		# Maximum execution time
		  NJOBS => 10,			# Max number of utility processes
		  WAITTIME => 3600,		# Nap length between idle() cycles

		  BACKEND_TYPE => undef,	# Backend type

		  TASKS => {},                  # Tasks in memory
		  TASKDIR => undef,             # Tasks to do. Value set in Agent::Dropbox
		  ARCHIVEDIR => undef,          # Jobs done. Value set in Agent::Dropbox
		  STATS => [],			# Historical stats.
		  MAX_TASKS => 15000,            # Max number of local tasks

		  LAST_CONNECT => 0,		# Last time connected and made known
		  LAST_WORK => time(),		# Last time we saw work
		  LAST_COMPLETED => 0,		# Last completed a task
		  DBH_LAST_USE => 0,		# Last use of the database

		  BOOTTIME => time(),		# Time this agent started
		  LOAD_DROPBOX_WORKDIRS	=> 1,	# This agent requires the working directories
		);
    my %args = (@_);
#   use 'defined' instead of testing on value to allow for arguments which are set to zero.
    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = $class->SUPER::new(%args);

    # Create a JobManager
    $self->{JOBMANAGER} = PHEDEX::Core::JobManager->new (
						NJOBS	=> $self->{NJOBS},
						VERBOSE	=> $self->{VERBOSE},
						DEBUG	=> $self->{DEBUG},
							);

    # Handle signals
    $SIG{INT} = $SIG{TERM} = sub { $self->{SIGNALLED} = shift;
				   $self->{JOBMANAGER}->killAllJobs() };

    eval ("use PHEDEX::Transfer::$args{BACKEND_TYPE}");
    do { chomp ($@); die "Failed to load backend: $@\n" } if $@;
    $self->{BACKEND} = eval("new PHEDEX::Transfer::$args{BACKEND_TYPE}(\$self)");
    do { chomp ($@); die "Failed to create backend: $@\n" } if $@;
    -d $$self{TASKDIR} || mkdir($$self{TASKDIR}) || -d $$self{TASKDIR}
        || die "$$self{TASKDIR}: cannot create: $!\n";
    -d $$self{ARCHIVEDIR} || mkdir($$self{ARCHIVEDIR}) || -d $$self{ARCHIVEDIR}
        || die "$$self{ARCHIVEDIR}: cannot create: $!\n";

    # If we have a notification-port, set it also in the environment, so it
    # can be picked up for use by other sessions (QMon etc) that may need it.
    # This is a little kludgy, in that it will affect all sessions in the
    # process that do not use the config-file to establish an environment.
    # Nonetheless, it's easier than tramping around a NOTIFICATION_PORT in
    # every session we create, so we stick with it.
    if ( $self->{NOTIFICATION_PORT} && !$ENV{PHEDEX_NOTIFICATION_PORT} )
    { $ENV{PHEDEX_NOTIFICATION_PORT} = $self->{NOTIFICATION_PORT}; }

    bless $self, $class;
    return $self;
}

#
# POE events
#

# Initialize all POE events this object handles
sub _poe_init
{
  my ($self, $kernel, $session) = @_[ OBJECT, KERNEL, SESSION ];
# $session->option(trace => 1);  $|++; # XXX Debugging

  $self->defineWorkflow();

  my @poe_subs = qw( advertise_self verify_tasks purge_lost_tasks
		     fill_backend maybe_disconnect
		     sync_tasks report_tasks update_tasks fetch_tasks
		     start_task finish_task
		     prevalidate_task prevalidate_done
		     predelete_task predelete_done
		     transfer_task transfer_done
		     postvalidate_task postvalidate_done
		     postdelete_task postdelete_done );

  $kernel->state($_, $self) foreach @poe_subs;

  $self->{BACKEND}->_poe_init($kernel, $session);

  # Get periodic events going
  $kernel->yield('advertise_self');
  $kernel->yield('verify_tasks');
  $kernel->yield('purge_lost_tasks');
  $kernel->yield('fill_backend');
  $kernel->yield('sync_tasks');
  $kernel->yield('maybe_disconnect');
}

# XXX TODO:  Find a more beutiful way to protect DB-interacting code from transient failures.

# advertise agent existence to the database
sub advertise_self
{
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
  $self->delay_max($kernel, 'advertise_self', 2400);
  eval {
      $self->reconnect();
  };
  $self->rollbackOnError();
}

# disconnect if we have nothing to do that requires the database
sub maybe_disconnect
{
    my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
    $self->delay_max($kernel, 'maybe_disconnect', 15);

eval
{
    # Detach from the database if the connection wasn't used
    # recently (at least one minute) and if it looks the agent
    # has enough work for some time and next synchronisation
    # is not imminent.
    # XXX TODO: also consider the existing transfer queue: only
    # disconnect if we have a lot of work
    my $now = &mytimeofday();
    if (defined $$self{DBH}
	&& $self->next_event_time('sync_tasks') - $now > 600
	&& $now - $$self{DBH_LAST_USE} > 60
	&& $now - $$self{LAST_WORK} > 4*3600)
    {
	$self->Logmsg("disconnecting from database");
	$self->disconnectAgent(1);
    }
}; $self->rollbackOnError();
}

# sync local task cache with database
sub sync_tasks
{
  my ( $self, $kernel, $session ) = @_[ OBJECT, KERNEL, SESSION ];
  $self->delay_max($kernel, 'sync_tasks', 1800);

  if ($self->connectionValid() || $self->reconnect()) {
      $kernel->call($session, 'report_tasks');
      $kernel->call($session, 'update_tasks');
      $kernel->call($session, 'fetch_tasks') if ! -f "$$self{DROPDIR}/drain";
      $$self{DBH_LAST_USE} = &mytimeofday();
  }
}

# Upload final task status to the database.
sub report_tasks
{
   my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

eval
{
   my $tasks = $self->{TASKS};

   my $rows = 0;
   my (%dargs, %eargs);
   my $dstmt = &dbprep($$self{DBH}, qq{
	insert into t_xfer_task_done
	(task, report_code, xfer_code, time_xfer, time_update)
	values (?, ?, ?, ?, ?)});
   my $estmt = &dbprep($$self{DBH}, qq{
	insert into t_xfer_error
	(to_node, from_node, fileid, priority, is_custodial,
	 time_assign, time_expire, time_export, time_inxfer, time_xfer,
         time_done, report_code, xfer_code, from_pfn, to_pfn, space_token,
	 log_xfer, log_detail, log_validate)
	values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)});

   foreach my $task (keys %$tasks) {
       next if ! $$tasks{$task}{FINISHED};

       my $arg = 1;
       push(@{$dargs{$arg++}}, $$tasks{$task}{TASKID});
       push(@{$dargs{$arg++}}, $$tasks{$task}{REPORT_CODE});
       push(@{$dargs{$arg++}}, $$tasks{$task}{XFER_CODE});
       push(@{$dargs{$arg++}}, $$tasks{$task}{TIME_XFER});
       push(@{$dargs{$arg++}}, $$tasks{$task}{TIME_UPDATE});

       # Log errors.  We ignore expired tasks
       if ($$tasks{$task}{REPORT_CODE} != PHEDEX_RC_SUCCESS &&
	   $$tasks{$task}{REPORT_CODE} != PHEDEX_RC_EXPIRED)
       {
	   # Truncate transfer logs longer than 100k characters,
	   # reporting only the initial and final part of the log
	   
	   my $xferlog = $$tasks{$task}{LOG_XFER};
	   if (length($xferlog) > 100_000) {
	       substr($xferlog,49_950,length($xferlog)-2*49_950)="\n[omitted ".(length($xferlog)-2*49_950)." characters from transfer log]\n";
	   }

	   my $arg = 1;
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TO_NODE_ID});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{FROM_NODE_ID});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{FILEID});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{PRIORITY});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{IS_CUSTODIAL});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TIME_ASSIGN});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TIME_EXPIRE});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TIME_EXPORT});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TIME_INXFER});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TIME_XFER});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TIME_UPDATE});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{REPORT_CODE});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{XFER_CODE});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{FROM_PFN});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{TO_PFN});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{SPACE_TOKEN});
	   push(@{$eargs{$arg++}}, $xferlog);
	   push(@{$eargs{$arg++}}, $$tasks{$task}{LOG_DETAIL});
	   push(@{$eargs{$arg++}}, $$tasks{$task}{LOG_VALIDATE});
       }

       if ((++$rows % 100) == 0)
       {
	   &dbbindexec($dstmt, %dargs);
	   &dbbindexec($estmt, %eargs) if %eargs;
	   $$self{DBH}->commit();
	   foreach my $t (@{$dargs{1}}) {
	       $self->Logmsg("uploaded status of task=$t") if $$self{VERBOSE};
	       $self->forgetTask($t);
	   }
	   %dargs = ();
	   %eargs = ();
       }
   }

   if (%dargs)
   {
       &dbbindexec($dstmt, %dargs);
       &dbbindexec($estmt, %eargs) if %eargs;
       $$self{DBH}->commit();
       foreach my $t (@{$dargs{1}}) {
	   $self->Logmsg("uploaded status of task=$t") if $$self{VERBOSE};
	   $self->forgetTask($t);
       }
   }
}; $self->rollbackOnError();
}

# Fetch new tasks from the database.
sub fetch_tasks
{
   my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

eval
{
   my $tasks = $self->{TASKS};
   my ($dest, %dest_args) = $self->myNodeFilter ("xt.to_node");
   my ($src, %src_args) = $self->otherNodeFilter ("xt.from_node");
   my $now = &mytimeofday();
   my (%pending, %busy, %fetched);

   # If we still have enough work (more than one third of max), leave.
   my $localtasks = scalar keys %$tasks;
   my $maxtasks = $self->{MAX_TASKS}/3;
   if ($localtasks >= $maxtasks) {
       $self->Logmsg("over $maxtasks pending tasks ($localtasks), not fetching more") if $self->{VERBOSE};
       return;
   }

   # Find out how many we have pending per link so we can throttle.
   ($pending{"$$_{FROM_NODE} -> $$_{TO_NODE}"} ||= 0)++
       for grep(! $$_{FINISHED}, values %$tasks);

   # Fetch new tasks.
   my $i = &dbprep($$self{DBH}, qq{
       insert into t_xfer_task_inxfer (task, time_update, from_pfn, to_pfn, space_token)
	   values (?,?,?,?,?)});
   my $q = &dbexec($$self{DBH}, qq{
	select
	    xt.id taskid, xt.fileid, xt.rank, xt.priority, xt.is_custodial,
	    f.logical_name, f.filesize, f.checksum,
	    xt.from_node from_node_id, ns.name from_node,
	    xt.to_node to_node_id, nd.name to_node,
	    xti.from_pfn, xti.to_pfn,
	    xt.time_assign, xt.time_expire,
	    xte.time_update time_export
	from t_xfer_task xt
	   join t_xfer_task_export xte on xte.task = xt.id
	   left join t_xfer_task_inxfer xti on xti.task = xt.id
	   left join t_xfer_task_done xtd on xtd.task = xt.id
	   join t_adm_node ns on ns.id = xt.from_node
	   join t_adm_node nd on nd.id = xt.to_node
	   join t_xfer_file f on f.id = xt.fileid
	where xti.task is null
	   and xtd.task is null
	   and xt.time_expire > :limit
	   and $dest $src
	order by time_assign asc, rank asc},
        ":limit" => $now + 3600, %dest_args, %src_args);

   my $rows=0;
   my %iargs;
   my %errors;
   while (my $row = $q->fetchrow_hashref())
   {

	# If we have just too much work, leave.
	last if scalar keys %$tasks >= $self->{MAX_TASKS};

	# If we have too many on this link, skip.
	my $linkkey = "$$row{FROM_NODE} -> $$row{TO_NODE}";
	if (($pending{$linkkey} || 0) >= 1000)
	{
            $self->Logmsg("link $linkkey already has $pending{$linkkey} pending"
		    . " tasks, not fetching more from the database")
		if ! $busy{$linkkey} && $$self{VERBOSE};
	    $busy{$linkkey} = 1;
	    next;
	}

	# Mark used in database.
        $row->{FROM_PROTOS} = [@{$self->{BACKEND}{PROTOCOLS}}];
        $row->{TO_PROTOS}   = [@{$self->{BACKEND}{PROTOCOLS}}];
	my $h;
	eval {
	    $h = makeTransferTask($self, $row, $self->{BACKEND}->{CATALOGUES} );
	};
	if ($@) {
	    chomp $@;
	    $errors{$@} ||= 0;
	    $errors{$@}++;
	    next;
	}

#	A sanity check, should not be needed but who knows...
	foreach ( qw / FROM_PFN TO_PFN FROM_NODE TO_NODE / )
	{
	  if ( !defined($h->{$_}) )
	  {
	    $self->Alert('No $_ in task: ',join(', ',map { "$_=$row->{$_}" } sort keys %{$row}));
	  }
	}
        map { $row->{$_} = $h->{$_} } keys %{$h};
	$row->{SPACE_TOKEN} = $h->{TO_TOKEN};
	$$row{TIME_INXFER} = $now;

	# Generate a local task descriptor.  It doesn't really matter
	# if things go badly wrong here, we'll clean it up in purge.
	last unless &output("$$self{TASKDIR}/$$row{TASKID}", Dumper($row));
	$$tasks{$$row{TASKID}} = $row;

	# If task descriptor was generated properly, add task to bulk update array
	my $arg=1;
	push(@{$iargs{$arg++}}, $$row{TASKID});
	push(@{$iargs{$arg++}}, $now);
	push(@{$iargs{$arg++}}, $$row{FROM_PFN});
	push(@{$iargs{$arg++}}, $$row{TO_PFN});
	push(@{$iargs{$arg++}}, $$row{SPACE_TOKEN});

        ($pending{$linkkey} ||= 0)++;
        ($fetched{$linkkey} ||= 0)++;
	
	if ((++$rows % 100) == 0)
	   {
	       &dbbindexec($i, %iargs);
	       $$self{DBH}->commit();
	       foreach my $t (@{$iargs{1}}) {
		   $self->Logmsg("set status to inxfer for task=$t") if $$self{VERBOSE};
	       }
	       %iargs = ();
	   }
    }
   
   if (%iargs)
   {
       &dbbindexec($i, %iargs);
       $$self{DBH}->commit();
       foreach my $t (@{$iargs{1}}) {
           $self->Logmsg("set status to inxfer for task=$t") if $$self{VERBOSE};
       }
   }
   
    # report error summary
    foreach my $err (keys %errors) {
	$self->Alert ("'$err' occurred for $errors{$err} tasks" );
	delete $errors{$err};
    }

   # report fetched summary
   if ($self->{VERBOSE}) {
       foreach my $linkkey (sort keys %fetched) {
	   $self->Logmsg("fetched $fetched{$linkkey} new tasks for link $linkkey");
       }
   }

   $q->finish(); # In case we left before going through all the results
}; $self->rollbackOnError();
}

# update expire time changes to tasks
# XXX TODO update task priority
sub update_tasks
{
   my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

eval
{
   my $tasks = $self->{TASKS};

    # Propagate extended expire time to local task copies.
    my ($dest, %dest_args) = $self->myNodeFilter ("xt.to_node");
    my ($src, %src_args) = $self->otherNodeFilter ("xt.from_node");

    my $qupdate = &dbexec($$self{DBH}, qq{
	select xt.id taskid, xt.time_expire
	from t_xfer_task xt
	  join t_xfer_task_inxfer xti on xti.task = xt.id
	  left join t_xfer_task_done xtd on xtd.task = xt.id
	where xtd.task is null and $dest $src},
        %dest_args, %src_args);

    while (my $row = $qupdate->fetchrow_hashref())
    {
	next if ! exists $$tasks{$$row{TASKID}};
	my $existing = $$tasks{$$row{TASKID}};
	next if $$existing{TIME_EXPIRE} >= $$row{TIME_EXPIRE};
	$self->Logmsg("task=$$existing{TASKID} expire time extended from "
		. join(" to ",
		       map { strftime('%Y-%m-%d %H:%M:%S', gmtime($_)) }
		       $$existing{TIME_EXPIRE}, $$row{TIME_EXPIRE}))
	    if $$self{VERBOSE};
	$$existing{TIME_EXPIRE} = $$row{TIME_EXPIRE};
	&output("$$self{TASKDIR}/$$existing{TASKID}", Dumper($existing));
    }
}; $self->rollbackOnError();
}

# Read in and verify all transfer tasks.
sub verify_tasks
{
    my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
    $self->delay_max($kernel, 'verify_tasks', 15);

    my $now = &mytimeofday();

    my @tasks;
    return if ! &getdir($$self{TASKDIR}, \@tasks);

    my $do_purge = 0;
    foreach my $taskid (@tasks)
    {
	my $info = &evalinfo("$$self{TASKDIR}/$taskid");
	if (! $info || $@)
	{
	    $self->Alert("garbage collecting corrupted transfer task=$taskid ($info, $@)");
	    $self->forgetTask($taskid);
	    $do_purge = 1;
	    next;
	}
	$self->{TASKS}->{$taskid} ||= $info;
	my $expired = $self->check_task_expire($taskid);
        $kernel->yield('finish_task', $taskid) if $expired;
    }

    # Create new time period with current statistics.
    $self->statsNewPeriod();

    $self->delay_max($kernel, 'purge_lost_tasks', 0) if $do_purge;

    # Remember current time if we are seeing work.
    $$self{LAST_WORK} = $now if %{$self->{TASKS}};
}

# Kill ghost transfers in the database and locally.
sub purge_lost_tasks
{
    my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
    $self->delay_max($kernel, 'purge_lost_tasks', 3600);

eval
{
    my $tasks = $self->{TASKS};
    my $now = &mytimeofday();

    # Come back later if we can't get a connection
    return unless ($self->connectionValid() || $self->reconnect());

    # Compare local transfer pool with database and reset everything one
    # or the other doesn't know about.  We assume this is the only agent
    # managing transfers for the links it's given.  If database has a
    # locally unknown transfer, we mark the database one lost.  If we
    # have a local transfer unknown to the database, we trash the local.
    my (%inlocal, %indb) = ();

    # Get the local transfer pool.  All we need is the task ids.
    $inlocal{$_} = 1 for keys %$tasks;

    # Get the database transfer pool.  Again, just task ids.
    my ($dest, %dest_args) = $self->myNodeFilter ("xt.to_node");
    my ($src, %src_args) = $self->otherNodeFilter ("xt.from_node");
    my $qpending = &dbexec($$self{DBH}, qq{
      select xti.task
      from t_xfer_task_inxfer xti
        join t_xfer_task xt on xt.id = xti.task
        left join t_xfer_task_done xtd on xtd.task = xti.task
      where xtd.task is null and $dest $src},
      %dest_args, %src_args);
    while (my ($taskid) =  $qpending->fetchrow())
    {
	$indb{$taskid} = 1;
    }

    # Calculate differences.
    my @lostdb = grep(! $indb{$_}, keys %inlocal);
    my @lostlocal = grep(! $inlocal{$_}, keys %indb);

    if ( @lostlocal )
    {
      $self->Alert("resetting database tasks lost locally: @{[sort @lostlocal]}"
	   . " (locally known: @{[sort keys %inlocal]})");
      # Mark locally unknown tasks as lost in database.
      my @now = (&mytimeofday()) x scalar @lostlocal;
      my $qlost = &dbprep($$self{DBH}, qq{
        insert into t_xfer_task_done
          (task, report_code, xfer_code, time_xfer, time_update)
	  values (?, @{[ PHEDEX_RC_LOST_TASK ]}, @{[ PHEDEX_XC_NOXFER ]}, -1, ?)});
      &dbbindexec($qlost, 1 => \@lostlocal, 2 => \@now);
      $$self{DBH}->commit();
    }

    # Remove locally known tasks forgotten by database.
    $self->Alert("resetting local tasks lost in database: @{[sort @lostdb]}"
	   . " (database known: @{[sort keys %indb]})")
	if @lostdb;
    foreach (@lostdb)
    {
	$self->forgetTask($_);
    }

    $$self{DBH_LAST_USE} = $now;
}; $self->rollbackOnError();
}

# Fill the backend with as many transfers as it can take.
#
# The files are assigned to the link on a fair share basis.  Here the
# fairness means that we try to keep every link with transfers to its
# maximum capacity while spreading the number of available "transfer
# slots" as fairly as possible over the links which have transfers.
#
# The algorithm works by assigning each link weights as a ratio of
# successfully completed transfers vs. number of transfer slots used
# by the link, over the observation period (some hours).  The final
# probability assigned to each link is the fraction of its weight
# from the total weight over all links.  Every link always gets a
# probability greater than a small "epsilon" value, which guarantees
# every link eventually gets a chance to be tried.
#
# This algorithm tends to give more files to links which get through
# more files, but sharing the avaiable backend job slots fairly.  The
# weighting by consumsed transfer slots is a key factor as it permits
# the agent to detect which links benefit from being given more files.
sub fill_backend
{
    my ( $self, $kernel, $session ) = @_[ OBJECT, KERNEL, SESSION ];
    $self->delay_max($kernel, 'fill_backend', 15);

    my $tasks = $self->{TASKS};

    my (%stats, %todo);
    my $now = &mytimeofday();
    my $nlinks = 0;

    # If the backend is busy, avoid doing heavy lifting.
    return if $$self{BACKEND}->isBusy();

    # Determine links with pending transfers.
    foreach my $t (values %$tasks)
    {
	# Do not conisder tasks which have started or have finished
	next if $t->{STARTED} || $t->{FINISHED};

	my $to = $$t{TO_NODE};
	my $from = $$t{FROM_NODE};

	# Consider this task.
	$nlinks++ if ! exists $todo{$to}{$from};
	push(@{$todo{$to}{$from}}, $t);
    }

    # Another quick exit if we have nothing to do.
    return if ! $nlinks;
    $self->Logmsg("balancing transfers on $nlinks links")
        if $$self{VERBOSE};

    # Determine link probability from recent usage.
    my $goodlinks = 0;
    foreach my $slot (@{$$self{STATS}})
    {
	foreach my $to (keys %{$$slot{LINKS}})
	{
	    foreach my $from (keys %{$$slot{LINKS}{$to}})
	    {
		my $s = $$slot{LINKS}{$to}{$from};
		# Add statistics based on link usage.
		$stats{$to}{$from} ||= { USED => 0, DONE => 0, ERRORS => 0 };
		$stats{$to}{$from}{DONE} += ($$s{DONE} || 0);
		$stats{$to}{$from}{USED} += ($$s{USED} || 0);
		$stats{$to}{$from}{ERRORS} += ($$s{ERRORS} || 0);

		# Count the "good" links: if something was accomplished and there weren't 100 errors
		if ($stats{$to}{$from}{DONE} && $stats{$to}{$from}{ERRORS} < 100) {
		    $goodlinks++;
		}
	    }
	}
    }

    my ($W, $wmin) = (0, 0.02 * $nlinks);
    my ($skippedlinks, $errlinks, $busylinks) = (0, 0, 0);
    foreach my $to (keys %todo)
    {
	foreach my $from (keys %{$todo{$to}})
	{
	    my $entry = $stats{$to}{$from} ||= {};

	    # Pass links with too many errors.
	    if (($$entry{ERRORS} || 0) > 100)
	    {
		$self->Logmsg("too many ($$entry{ERRORS}) recent errors on ",
			      "link $from -> $to, not allocating transfers")
		    if $$self{VERBOSE};
		delete $todo{$to}{$from};
		$skippedlinks++; $errlinks++;
		next;
	    }

	    # Pass links which are busy
	    if ( $$self{BACKEND}->isBusy ($from, $to) ) {
		$self->Logmsg("link $from -> $to is busy at the moment, ",
			      "not allocating transfers")
		    if $$self{VERBOSE};
		delete $todo{$to}{$from};
		$skippedlinks++; $busylinks++;
		next;
	    }

	    # Give links the weight of one if they have not been used.
	    if (! $$entry{USED})
	    {
		$$entry{W} = 1.0;
	    }

	    # Otherwise the weight is DONE/USED.
	    else
	    {
	        $$entry{W} = (1.0 * $$entry{DONE} / $$entry{USED});
	    }

	    # But if the weight is smaller than ~5 files/hour, clamp
	    # to that limit to guarantee minimum probability value.
	    $$entry{W} = $wmin if $$entry{W} < $wmin;

	    # Update total weight.
	    $W += $$entry{W};
	}
    }

    if ($skippedlinks == $nlinks) {
	# If we have nothing to do because all the links have too many
	# errors, then check if there were any recent transfers on
	# good links, and sync faster if there was
	if ($errlinks == $nlinks && $goodlinks 
	    && $self->next_event_time('sync_tasks') - $now > 300) {
	    $self->delay_max($kernel, 'sync_tasks', 300);
	    $self->Logmsg("all links were skipped due to errors, scheduling ",
			  "next synchronisation in five minutes")
		if $$self{VERBOSE};
	}
	return; # Nothing to do.
    }

    my @P;
    foreach my $to (sort keys %todo)
    {
	foreach my $from (sort keys %{$todo{$to}})
	{
	    # Compute final link probablity function.
	    my $low = (@P ? $P[$#P]{HIGH} : 0);
	    my $high = $low + $stats{$to}{$from}{W}/$W;
	    push(@P, { LOW => $low, HIGH => $high, TO => $to, FROM => $from });

            $self->Logmsg("link parameters for $from -> $to:"
		    . sprintf(' P=[%0.3f, %0.3f),', $P[$#P]{LOW}, $P[$#P]{HIGH})
		    . sprintf(' W=%0.3f,', $stats{$to}{$from}{W})
		    . " USED=@{[$stats{$to}{$from}{USED} || 0]},"
		    . " DONE=@{[$stats{$to}{$from}{DONE} || 0]},"
		    . " ERRORS=@{[$stats{$to}{$from}{ERRORS} || 0]}")
                if $$self{VERBOSE};
	}
    }

    # For each available job slot, determine which link should have
    # the transfers based on the probability function calculated from
    # the link statistics.  Then fill the job slot from the transfers
    # tasks on that link, in the order of task priority.

    # Select a link that merits to get the files.
    my ($i, $p) = (0, rand());
    $i++ while ($i < $#P && $p >= $P[$i]{HIGH});
    my $to = $P[$i]{TO};
    my $from = $P[$i]{FROM};

    # Sort the tasks according to priority.  Older tasks first, lower
    # rank first.
    # FIXME:  Sort when fetching tasks, not every fill_backend call
    $todo{$to}{$from} =
	[ sort { $$a{TIME_ASSIGN} <=> $$b{TIME_ASSIGN}
		 || $$a{RANK} <=> $$b{RANK} }
	  @{$todo{$to}{$from}} ];

    # Send files to transfer.
    # Note we use synchronous calls here in order to avoid race conditions with this function.
    my ($jobid, $jobdir, $jobtasks) = $kernel->call($session, 'start_batch', $todo{$to}{$from});

    if ($jobid) {
	foreach my $task ( values %$jobtasks ) {
	    $kernel->call($session, 'start_task', $task->{TASKID}, { JOBID => $jobid, JOBDIR => $jobdir });
	}

	$self->Logmsg("copy job $jobid assigned to link $from -> $to with "
		      . scalar(keys %$jobtasks) . " tasks and "
		      . sprintf('p=%0.3f and W=%0.3f and ', $p, $stats{$to}{$from}{W})
		      . scalar(@{$todo{$to}{$from}})
		      . " tasks in queue")
	    if $$self{VERBOSE};
	
	$self->Dbgmsg("copy job $jobid tasks: ", join(' ', sort keys %$jobtasks)) if $self->{DEBUG};
    }

    my $linkexhausted = @{$todo{$to}{$from}} ? 0 : 1;

    # Fill again as soon as possible
    $kernel->yield('fill_backend');

    # If we exhausted all transfer tasks on a link, make sure the next
    # synchronisation will occur relatively soon.  If we exhausted
    # tasks on all links, synchronise immediately.  This applies only
    # on transition from having tasks to not having them (only), so we
    # are not forcing continuous unnecessary reconnects.
    if ( $nlinks - $linkexhausted == 0)
    {
	$self->delay_max($kernel, 'sync_tasks', 0);
	$self->Logmsg("ran out of tasks, scheduling immediate synchronisation")
	    if $$self{VERBOSE};
    }
    elsif ($linkexhausted && $self->next_event_time('sync_tasks') - $now > 300)
    {
	$self->delay_max($kernel, 'sync_tasks', 300);
	$self->Logmsg("ran out of tasks on link $from -> $to, scheduling"
		. " next synchronisation in five minutes")
	    if $$self{VERBOSE};
    }
}

# start the transfer workflow for a task
sub start_task
{
    my ( $self, $kernel, $taskid, $taskargs ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    $self->Dbgmsg("start_task task=$taskid") if $self->{DEBUG};
    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    if ($taskargs) { $task->{$_} = $taskargs->{$_} for keys %$taskargs }
    $task->{STARTED} = &mytimeofday();
    $self->saveTask($taskid) || return;
    $kernel->yield( $self->next_subtask(), $taskid );
}

sub prevalidate_task
{
    my ( $self, $session, $taskid ) = @_[ OBJECT, SESSION, ARG0 ];

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $jobpath = $task->{JOBDIR};
    my $log = "$jobpath/T${taskid}-prevalidate-log";

    $self->{JOBMANAGER}->addJob( $session->postback('prevalidate_done', $taskid),
				 { PRIORITY => 4, TIMEOUT => $$self{TIMEOUT}, LOGFILE => $log },
				 @{$$self{VALIDATE_COMMAND}}, "pre",
				 @$task{qw(TO_PFN FILESIZE CHECKSUM)}, &boolean_yesno($task->{IS_CUSTODIAL}));
}

sub prevalidate_done
{
    my ( $self, $kernel, $context, $args) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    my ($taskid) = @$context;
    my ($jobargs) = @$args;

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $statcode = &numeric_statcode($jobargs->{STATUS});
    my $log = &input($jobargs->{LOGFILE});
    my $time_end = &mytimeofday();

    $task->{PREVALIDATE_CODE} = $statcode;

    my $done = 0;

    # if the pre-validation returned success, this file is already there.  mark success
    if ($statcode == PHEDEX_VC_SUCCESS)
    {
	$$task{REPORT_CODE} = PHEDEX_RC_SUCCESS;
	$$task{XFER_CODE} = PHEDEX_XC_NOXFER;
	$$task{LOG_DETAIL} = 'file validated before transfer attempt';
	$$task{LOG_XFER} = 'no transfer was attempted';
	$$task{LOG_VALIDATE} = $log;
	$$task{TIME_UPDATE} = $time_end;
	$$task{TIME_XFER} = -1;
	$done = 1;
    }
    # if the pre-validation returned 86 (PHEDEX_VC_VETO), the
    # transfer is vetoed, throw this task away.
    # see http://www.urbandictionary.com/define.php?term=eighty-six
    # or google "eighty-sixed".
    # We set the REPORT_CODE to -86 (PHEDEX_RC_VETO) so that
    # the error is counted as a "PhEDEx error", not a transfer
    # error, so it will not count against the link in the
    # backoff algorithms.
    elsif ($statcode == PHEDEX_VC_VETO)
    {
	$$task{REPORT_CODE} = PHEDEX_RC_VETO;
	$$task{XFER_CODE} = PHEDEX_XC_NOXFER;
	$$task{LOG_DETAIL} = 'file pre-validation vetoed the transfer';
	$$task{LOG_XFER} = 'no transfer was attempted';
	$$task{LOG_VALIDATE} = $log;
	$$task{TIME_UPDATE} = $time_end;
	$$task{TIME_XFER} = -1;
	$done = 1;
    }
    $self->saveTask($taskid) || return;

    $done ? $kernel->yield('finish_task', $taskid)
	  : $kernel->yield($self->next_subtask(), $taskid);
}

sub predelete_task
{
    my ( $self, $session, $taskid ) = @_[ OBJECT, SESSION, ARG0 ];

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $jobpath = $task->{JOBDIR};
    my $log = "$jobpath/T${taskid}-predelete-log";

    $self->{JOBMANAGER}->addJob( $session->postback('predelete_done', $taskid ),
				 { PRIORITY => 3, TIMEOUT => $self->{TIMEOUT}, LOGFILE => $log },
				 @{$self->{DELETE_COMMAND}}, "pre",
				 @$task{ qw(TO_PFN) });
}

sub predelete_done
{
    my ( $self, $kernel, $context, $args ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    my ($taskid) = @$context;
    my ($jobargs) = @$args;

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    $task->{PREDELETE_CODE} = &numeric_statcode($jobargs->{STATUS});
    $self->saveTask( $taskid ) || return;
    $kernel->yield($self->next_subtask(), $taskid);
}

# Marks a task as ready to transfer.  The transfer will begin when the
# backend detects that all tasks in a job are ready.
sub transfer_task
{
    my ( $self, $kernel, $taskid) = @_[ OBJECT, KERNEL, ARG0 ];

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    $task->{READY} = &mytimeofday();
    $self->saveTask($taskid) || return;
}

sub transfer_done
{
    my ( $self, $kernel, $taskid, $xferinfo ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    
    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;

    # copy results into the task
    $$task{XFER_CODE}  = &numeric_statcode($xferinfo->{STATUS});
    $$task{LOG_DETAIL} = $xferinfo->{DETAIL} || '';
    $$task{LOG_XFER}   = $xferinfo->{LOG}    || '';
    $$task{TIME_XFER}  = $xferinfo->{START}  || -1;
    $self->saveTask($taskid) || return;

    # save the transfer info
    &output("$task->{JOBDIR}/T${taskid}-xferinfo", Dumper($xferinfo));

    $kernel->yield($self->next_subtask(), $taskid);
}

sub postvalidate_task
{
    my ( $self, $session, $taskid ) = @_[ OBJECT, SESSION, ARG0 ];

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $jobpath = $task->{JOBDIR};
    my $log = "$jobpath/T${taskid}-postvalidate-log";

    $self->{JOBMANAGER}->addJob( $session->postback('postvalidate_done', $taskid),
				 { PRIORITY => 2, TIMEOUT => $$self{TIMEOUT}, LOGFILE => $log },
				 @{$$self{VALIDATE_COMMAND}}, $task->{XFER_CODE},
				 @$task{qw(TO_PFN FILESIZE CHECKSUM)}, &boolean_yesno($task->{IS_CUSTODIAL}));
}

sub postvalidate_done
{
    my ( $self, $kernel, $context, $args ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    my ($taskid) = @$context;
    my ($jobargs) = @$args;

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $statcode = &numeric_statcode($$jobargs{STATUS});
    my $log = &input($jobargs->{LOGFILE});
    my $done = $statcode == PHEDEX_VC_SUCCESS ? 1 : 0;

    # Set the report code.  This is the final arbitrator of transfer task success/failure.
    # Negative values are treated as an "acceptable error", and do not result in penalties.
    # The report code is:
    #   equal to the validation status if XFER_CODE was defined and positive
    #   equal to the validation status if the validation status was negative or zero
    #   equal to -255 - the validation status otherwise
    # The idea is that a transfer task should not be an error if the
    # transfer itself was not an error (XFER_CODE negative)
    # We move the "converted" validation status codes to be less than
    # -255 to distinguish them from PhEDEx-generated errors, which are
    # in the [-255,-1] range.
    my $reportcode = ((defined $$task{XFER_CODE} && $$task{XFER_CODE} >= 0) 
		      || $statcode <= 0) ? $statcode 
		                         : -255 - $statcode;

    # Set task parameters
    $$task{POSTVALIDATE_CODE} = $statcode;
    $$task{REPORT_CODE}  = $reportcode;
    $$task{LOG_VALIDATE} = $log || '';
    $$task{TIME_UPDATE}  = &mytimeofday();
    $self->saveTask($taskid) || return;

    $done ? $kernel->yield('finish_task', $taskid)
	  : $kernel->yield($self->next_subtask(), $taskid);
}

sub postdelete_task
{
    my ( $self, $session, $taskid ) = @_[ OBJECT, SESSION, ARG0 ];

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $jobpath = $task->{JOBDIR};
    my $log = "$jobpath/T${taskid}-postdelete-log";

    $self->{JOBMANAGER}->addJob( $session->postback('postdelete_done', $taskid),
				 { PRIORITY => 1, TIMEOUT => $self->{TIMEOUT}, LOGFILE => $log },
				 @{$self->{DELETE_COMMAND}}, "post",
				 @$task{ qw(TO_PFN) });
}

sub postdelete_done
{
    my ( $self, $kernel, $context, $args ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    my ($taskid) = @$context;
    my ($jobargs) = @$args;

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    $task->{POSTDELETE_CODE} = &numeric_statcode($jobargs->{STATUS});
    $self->saveTask( $taskid ) || return;
    $kernel->yield($self->next_subtask(), $taskid);
}

# Mark a task completed.  Brings the next synchronisation into next
# fifteen minutes, and updates statistics for the current period.
sub finish_task
{
    my ( $self, $kernel, $taskid ) = @_[ OBJECT, KERNEL, ARG0 ];
    $self->Dbgmsg("finish_task task=$taskid") if $self->{DEBUG};
    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return;
    my $now = &mytimeofday();

    # Set report code if it wasn't already set, in order of preference
    $$task{REPORT_CODE} = $$task{POSTVALIDATE_CODE} unless defined $$task{REPORT_CODE};
    $$task{REPORT_CODE} = $$task{XFER_CODE}         unless defined $$task{REPORT_CODE};
    $$task{REPORT_CODE} = $$task{PREVALIDATE_CODE}  unless defined $$task{REPORT_CODE};
    $$task{JOBLOG}      = $$task{JOBID} ? "$$self{ARCHIVEDIR}/$$task{JOBID}" : undef;
    $$task{FINISHED}    = $$task{TIME_UPDATE} = $now;

    # Save it
    $self->saveTask($taskid) || return;

    # If next synchronisation is too far away, pull it forward.
    $self->delay_max($kernel, 'sync_tasks', 900);

    $$self{LAST_COMPLETED} = $now;

    # Update statistics for the current period.
    my ($from, $to, $code) = @$task{"FROM_NODE", "TO_NODE", "REPORT_CODE"};
    my $s = $$self{STATS_CURRENT}{LINKS}{$to}{$from}
        ||= { DONE => 0, USED => 0, ERRORS => 0 };
    $$s{ERRORS}++ if $code > 0;
    $$s{DONE}++ if $code == 0;

    # Report but simplify download detail/validate logs.
    my $detail = $$task{LOG_DETAIL} || '';
    my $validate = $$task{LOG_VALIDATE} || '';
    foreach my $log (\$detail, \$validate)
    {
	$$log =~ s/^[-\d: ]*//gm;
	$$log =~ s/^[A-Za-z]+(\[\d+\]|\(\d+\)): //gm;
	$$log =~ s/\n+/ ~~ /gs;
	$$log =~ s/\s+/ /gs;
    }

    $self->Logmsg("xstats:"
	    . " task=$$task{TASKID}"
	    . " file=$$task{FILEID}"
	    . " from=$$task{FROM_NODE}"
	    . " to=$$task{TO_NODE}"
	    . " priority=$$task{PRIORITY}"
	    . " report-code=$$task{REPORT_CODE}"
	    . " xfer-code=$$task{XFER_CODE}"
	    . " size=$$task{FILESIZE}"
	    . " t-expire=$$task{TIME_EXPIRE}"
	    . " t-assign=$$task{TIME_ASSIGN}"
	    . " t-export=$$task{TIME_EXPORT}"
	    . " t-inxfer=$$task{TIME_INXFER}"
	    . " t-xfer=$$task{TIME_XFER}"
	    . " t-done=$$task{TIME_UPDATE}"
	    . " lfn=$$task{LOGICAL_NAME}"
	    . " from-pfn=$$task{FROM_PFN}"
	    . " to-pfn=$$task{TO_PFN}"
	    . " detail=($detail)"
	    . " validate=($validate)"
	    . " job-log=@{[$$task{JOBLOG} || '(no job)']}");

    # Indicate success.
    return 1;
}

#
# Utility functions (non-POE events)
#

# Define the workflow. This must be called after both command-line and default arguments are processed!
sub defineWorkflow
{
    my ($self) = @_;

    my $do_preval = ($$self{VALIDATE_COMMAND} && $$self{PREVALIDATE}) ? 1 : 0;
    my $do_predel = ($$self{DELETE_COMMAND} && $$self{PREDELETE}) ? 1 : 0;
    my $do_postval = $$self{VALIDATE_COMMAND} ? 1 : 0;
    my $do_postdel = $$self{DELETE_COMMAND} ? 1 : 0;

    # Define the task workflow
    my @workflow;
    push @workflow, 'start_task';
    push @workflow, 'prevalidate_task', 'prevalidate_done'   if $do_preval;
    push @workflow, 'predelete_task', 'predelete_done'       if $do_predel;
    push @workflow, 'transfer_task', 'transfer_done';
    push @workflow, 'postvalidate_task', 'postvalidate_done' if $do_postval;
    push @workflow, 'postdelete_task', 'postdelete_done'     if $do_postdel;
    push @workflow, 'finish_task';

    for(my $i=0; $i < $#workflow; $i++) {
	my $subtask = $workflow[$i];
	$self->{WORKFLOW}->{$subtask}->{ORDER} = $i;
	$self->{WORKFLOW}->{$subtask}->{NEXT}  = $i+1 <= $#workflow ? $workflow[$i+1] : undef;
	$self->{WORKFLOW}->{$subtask}->{PREV}  = $i-1 >= 0 ? $workflow[$i-1] : undef;
    }

    $self->Logmsg("task workflow: @workflow") if $self->{VERBOSE};
}

# Returns the next subtask based on the task workflow.  If the current
# subtask is not passed, then it is looked up based on the context of
# the calling subroutine.
sub next_subtask
{
    my ($self, $current) = @_;
    $current ||= (caller(1))[3];
    $current =~ s/.*::([^:]+)$/$1/ if defined $current;
    return undef if !defined $current;
    return defined $self->{WORKFLOW}->{$current}->{NEXT} ?
	$self->{WORKFLOW}->{$current}->{NEXT} : undef;
}

# schedule $event to occur AT MOST $maxdelta seconds into the future.
# if the event is already scheduled to arrive before that time,
# nothing is done.  returns the timestamp of the next event
sub delay_max
{
    my ($self, $kernel, $event, $maxdelta) = @_;
    my $now = &mytimeofday();
    my $id = $self->{ALARMS}->{$event}->{ID};
    my $next = $kernel->alarm_adjust($id, 0);
    if (!$next) {
	$next = $now + $maxdelta;
	$id = $kernel->alarm_set($event, $next);
    } elsif ($next - $now > $maxdelta) {
	$next = $kernel->alarm_adjust($id, $now - $next + $maxdelta);
    }
    $self->{ALARMS}->{$event} = { ID => $id, NEXT => $next };
    return $next;
}

# return the timestamp of the next scheduled $event (must be set using delay_max())
# returns undef if there is no event scheduled.
sub next_event_time
{
    my ($self, $event) = @_;
    return $self->{ALARMS}->{$event}->{NEXT};
}

# If stopped, tell backend to stop, then wait for all the pending
# utility jobs to complete.  All backends just abandon the jobs, and
# we try to pick up on the transfer again if the agent is restarted.
# Utility jobs usually run quickly so we let them run to completion.
sub stop
{
    my ($self) = @_;
    $self->{BACKEND}->stop();
    # TODO:  Actually allow utility jobs to run to completion...
}

# turn a JobManager job STATUS into a number
# in case of a job being hangup/terminated/killed, STATUS is, e.g.  "signal 1"
sub numeric_statcode
{
    my ($statcode) = @_;
    return undef unless defined $statcode;
    return ($statcode =~ /^-?\d+$/ ? $statcode : 128 + ($statcode =~ /(\d+)/)[0]);
}

# turn a 'y' or 'n' value into a boolean number
sub boolean_yesno
{
    my ($yn) = @_;
    return undef if !defined $yn || $yn !~ /^[yn]$/;
    return ($yn eq 'y' ? 1 : 0);
}

# Reconnect the agent to the database.  If the database connection
# has been shut, create a new connection.  Update agent status.  Set
# $$self{DBH} to database handle and $$self{NODES_ID} to hash of the
# (node name, id) pairs.
sub reconnect
{
    my ($self) = @_;

    my $rv = 0;
eval
{
    my $now = &mytimeofday();

    # Now connect.
    my $dbh = $self->connectAgent();
    my @nodes = $self->expandNodes();
    unless (@nodes) { die("Cannot find nodes in database for '@{$$self{NODES}}'") };

    # Indicate to file router which links are "live."
    my ($dest, %dest_args) = $self->myNodeFilter ("l.to_node");
    my ($src, %src_args) = $self->otherNodeFilter ("l.from_node");
    my @protos = $$self{BACKEND}->protocols();

    &dbexec($dbh, qq{
	delete from t_xfer_sink l where $dest $src},
	%dest_args, %src_args);
    &dbexec($dbh, qq{
	insert into t_xfer_sink (from_node, to_node, protocols, time_update)
	select l.from_node, l.to_node, :protos, :now from t_adm_link l
	where $dest $src},
	":protos" => "@protos", ":now" => $now, %dest_args, %src_args);
    $dbh->commit();

    $$self{DBH_LAST_USE} = $now;
    $$self{LAST_CONNECT} = $now;
    $rv = 1; # reconnected OK
}; $self->rollbackOnError();
return $rv;
}

# Save a task after change of status.
sub saveTask
{
    my ($self, $taskid) = @_;
    my $task = $self->getTask($taskid) || return 0;
    return &output("$$self{TASKDIR}/$$task{TASKID}", Dumper($task));
}

# Get a task from memory by its ID
sub getTask
{
    my ($self, $taskid) = @_;
    return undef if !$taskid;
    if (!exists $self->{TASKS}->{$taskid}) {
	return undef;
    }
    my $task = $self->{TASKS}->{$taskid};
    if (!exists $task->{TASKID} || $task->{TASKID} != $taskid) {
	return undef;
    }
    return $task;
}

# forget that a task ever existed.  always returns true.
sub forgetTask
{
    my ($self, $taskid) = @_;
    my $taskfile = "$$self{TASKDIR}/$taskid";
    unlink($taskfile) if -e $taskfile;
    my $task = delete $self->{TASKS}->{$taskid} if exists $self->{TASKS}->{$taskid};
    $task->{FORGOTTEN} = &mytimeofday();  # a signal to backend job references of this task
    return 1;
}

# Start a new statistics period.  If we have more than the desired
# amount of statistics periods, remove old ones.
sub statsNewPeriod
{
    my ($self) = @_;
    my $tasks = $self->{TASKS};
    my $now = &mytimeofday();

    # Prune recent history.
    $$self{STATS} = [ grep($now - $$_{TIME} <= 3600, @{$$self{STATS}}) ];

    # Add new period.
    my $current = $$self{STATS_CURRENT} = { TIME => $now, LINKS => {} };
    push(@{$$self{STATS}}, $current);

    # Add statistics on transfer slots used.
    foreach my $t (values %$tasks)
    {
	# Skip if the transfer task was completed or hasn't started.
	next if $$t{FINISHED} || !$$t{STARTED};

	# It's using up a transfer slot, add to time slot link stats.
	my ($from, $to) = @$t{"FROM_NODE", "TO_NODE"};
	$$current{LINKS}{$to}{$from} ||= { DONE => 0, USED => 0, ERRORS => 0 };
	$$current{LINKS}{$to}{$from}{USED}++;
    }
}

# Check if a task is expired.  If it is expired, set 
sub check_task_expire
{
    my ( $self, $taskid ) = @_;

    my $task = $self->getTask($taskid) || $self->forgetTask($taskid) && return 0;
    my $now = &mytimeofday();

    # Do not expire tasks which have started or have finished
    return 0 if $$task{STARTED} || $$task{FINISHED}; 

    # If it has already expired, just remove it.
    my $prettyhours = sprintf "%0.1fh ", ($now - $$task{TIME_ASSIGN})/3600;
    if ($now >= $$task{TIME_EXPIRE})
    {
	$self->Logmsg("PhEDEx transfer task $$task{TASKID} has expired after $prettyhours, discarding");
	$self->forgetTask($taskid);
    }
    # If the task is too near expiration, mark it failed.
    elsif ($now >= $$task{TIME_EXPIRE} - 1200)
    {
	$self->Logmsg("PhEDEx transfer task $$task{TASKID} was nearly expired after $prettyhours, discarding");
	$$task{XFER_CODE}    = PHEDEX_XC_NOXFER;
	$$task{REPORT_CODE}  = PHEDEX_RC_EXPIRED;
	$$task{LOG_DETAIL}   = "transfer expired in the PhEDEx download agent queue after $prettyhours";
	$$task{LOG_XFER}     = "no transfer was attempted";
	$$task{LOG_VALIDATE} = "no validation was attempted";
	$$task{TIME_UPDATE}  = $now;
	$$task{TIME_XFER}    = -1;
	$$task{STARTED}      = -1;  # prevent this task from being started
	return 1;
    }
    
    # OK, it's not expired
    return 0;
}

1;
