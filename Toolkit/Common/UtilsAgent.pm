package UtilsAgent;
use strict;
require Exporter;
use POSIX;
use UtilsCommand;
use UtilsLogging;
use UtilsTiming;
use UtilsRFIO;
use base 'Exporter';

sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my %args = (@_);
    my $me = $0; $me =~ s|.*/||;
    die "$me: fatal error: no drop box directory given\n" if ! $args{DROPDIR};
    die "$me: fatal error: non-existent drop box directory\n" if ! -d $args{DROPDIR};
    foreach my $dir (@{$args{NEXTDIR}}) {
	if ($dir =~ /^([a-z]+):/) {
            die "$me: fatal error: unrecognised bridge $1" if ($1 ne "scp" && $1 ne "rfio");
	} else {
            die "$me: fatal error: no downstream drop box\n" if ! -d $dir;
	}
    }

    my $self = {
	ME => $me,
	DROPDIR => $args{DROPDIR},
	NEXTDIR => $args{NEXTDIR},
	INBOX => "$args{DROPDIR}/inbox",
	WORKDIR => "$args{DROPDIR}/work",
	OUTDIR => "$args{DROPDIR}/outbox",
	STOPFLAG => "$args{DROPDIR}/stop",
	PIDFILE => "$args{DROPDIR}/pid",
	WAITTIME => $args{WAITTIME} || 7,
	JUNK => {},
	BAD => {},
	STARTTIME => [],
	NWORKERS => $args{NWORKERS} || 0,
	WORKERS => undef
    };
    bless $self, $class;

    if (-f $self->{STOPFLAG}) {
	&warn("removing (old?) stop flag");
	unlink ($self->{STOPFLAG});
    }

    if (-f $self->{PIDFILE}) {
	my $oldpid = `cat $self->{PIDFILE}`;
	chomp ($oldpid);
	&warn("removing (old?) pidfile ($oldpid)");
	unlink ($self->{PIDFILE});
    }

    &output ($self->{PIDFILE}, "$$\n")
	or die "$me: fatal error: cannot write to $self->{PIDFILE}: $!\n";

    -d $self->{INBOX} || mkdir $self->{INBOX}
	|| die "$me: fatal error: cannot create inbox: $!\n";
    -d $self->{WORKDIR} || mkdir $self->{WORKDIR}
	|| die "$me: fatal error: cannot create work directory: $!\n";
    -d $self->{OUTDIR} || mkdir $self->{OUTDIR}
	|| die "$me: fatal error: cannot create outbox directory: $!\n";

    return $self;
}

# User hook
sub init
{}

sub initWorkers
{
    my $self = shift;
    return if ! $self->{NWORKERS};
    $self->{WORKERS} = [ (0) x $self->{NWORKERS} ];
    $self->checkWorkers();
}

# Ensure workers are running, if not, restart them
sub checkWorkers
{
    my $self = shift;
    my $nworkers = scalar @{$self->{WORKERS}};
    for (my $i = 0; $i < $nworkers; ++$i)
    {
	if (! $self->{WORKERS}[$i] || waitpid($self->{WORKERS}[$i], WNOHANG) > 0) {
	    my ($old, $new) = ($self->{WORKERS}[$i], $self->startWorker ($i));
	    $self->{WORKERS}[$i] = $new;

	    if (! $old) {
		&logmsg ("worker $i ($new) started");
	    } else {
	        &logmsg ("worker $i ($old) stopped, restarted as $new");
	    }
	}
    }
}

# Start a worker
sub startWorker
{
    my $self = shift;
    die "derived class asked for workers, but didn't override startWorker!\n";
}

# Stop workers
sub stopWorkers
{
    my $self = shift;
    return if ! $self->{NWORKERS};

    # Make workers quit
    my @workers = @{$self->{WORKERS}};
    &note ("stopping worker threads");
    &touch (map { "$self->{DROPDIR}/worker-$_/stop" } 0 .. $#workers);
    while (scalar @workers)
    {
	my $pid = waitpid (-1, 0);
	@workers = grep ($pid ne $_, @workers);
	&logmsg ("child process $pid exited, @{[scalar @workers]} workers remain") if $pid > 0;
    }
}

# Pick least-loaded worker
sub pickWorker
{
    my ($self) = @_;
    my $nworkers = scalar @{$self->{WORKERS}};
    my $basedir = $self->{DROPDIR};
    return (sort { $a->[1] <=> $b->[1] }
    	    map { [ $_, scalar @{[<$basedir/worker-$_/{inbox,work}/*>]} ] }
	    0 .. $nworkers-1) [0]->[0];
}

# Check if the agent should stop.  If the stop flag is set, cleans up
# and quits.  Otherwise returns.
sub maybeStop
{
    my $self = shift;

    # Check for the stop flag file.  If it exists, quit: remove the
    # pidfile and the stop flag and exit.
    return if ! -f $self->{STOPFLAG};

    &note("exiting from stop flag");
    unlink($self->{PIDFILE});
    unlink($self->{STOPFLAG});
    $self->stopWorkers() if $self->{NWORKERS};
    $self->stop();
    exit (0);
}

# User hook
sub stop {}

# Look for pending drops in inbox.
sub readInbox
{
    my $self = shift;

    die "$self->{ME}: fatal error: no inbox directory given\n" if ! $self->{INBOX};

    # Scan the inbox.  If this fails, file an alert but keep going,
    # the problem might be transient (just sleep for a while).
    my @files = ();
    &alert("cannot list inbox: $!")
	if (! &getdir($self->{INBOX}, \@files));

    # Check for junk
    foreach my $f (@files)
    {
	# Make sure we like it.
	if (! -d "$self->{INBOX}/$f")
	{
	    &alert("junk ignored in inbox: $f") if ! exists $self->{JUNK}{$f};
	    $self->{JUNK}{$f} = 1;
        }
	else
	{
	    delete $self->{JUNK}{$f};
        }
    }

    # Return those that are ready
    return grep(-f "$self->{INBOX}/$_/go", @files);
}

# Look for pending tasks in the work directory.
sub readPending
{
    my $self = shift;
    die "$self->{ME}: fatal error: no work directory given\n" if ! $self->{WORKDIR};

    # Scan the work directory.  If this fails, file an alert but keep
    # going, the problem might be transient.
    my @files = ();
    &alert("cannot list workdir: $!")
	if (! getdir($self->{WORKDIR}, \@files));

    return @files;
}

# Look for tasks waiting for transfer to next agent.
sub readOutbox
{
    my $self = shift;
    die "$self->{ME}: fatal error: no outbox directory given\n" if ! $self->{OUTDIR};

    # Scan the outbox directory.  If this fails, file an alert but keep
    # going, the problem might be transient.
    my @files = ();
    &alert("cannot list outdir: $!")
	if (! getdir ($self->{OUTDIR}, \@files));

    return @files;
}

# Rename a drop to a new name
sub renameDrop
{
    my ($self, $drop, $newname) = @_;
    &mv ("$self->{WORKDIR}/$drop", "$self->{WORKDIR}/$newname")
        || do { &alert ("can't rename $drop to $newname"); return 0; };
    return 1;
}

# Utility to undo from failed scp bridge operation
sub scpBridgeFailed
{
    my ($msg, $remote) = @_;
    # &runcmd ("ssh", $host, "rm -fr $remote");
    &alert ($msg);
    return 0;
}

sub scpBridgeDrop
{
    my ($source, $target) = @_;

    return &scpBridgeFailed ("failed to chmod $source", $target)
        if ! chmod(0775, "$source");

    return &scpBridgeFailed ("failed to copy $source", $target)
        if &runcmd ("scp", "-rp", "$source", "$target");

    return &scpBridgeFailed ("failed to copy /dev/null to $target/go", $target)
        if &runcmd ("scp", "/dev/null", "$target/go"); # FIXME: go-pending?

    return 1;
}

# Utility to undo from failed rfio bridge operation
sub rfioBridgeFailed
{
    my ($msg, $remote) = @_;
    &rfrmall ($remote) if $remote;
    &alert ($msg);
    return 0;
}

sub rfioBridgeDrop
{
    my ($source, $target) = @_;
    my @files = <$source/*>;
    do { &alert ("empty $source"); return 0; } if ! scalar @files;

    return &rfioBridgeFailed ("failed to create $target")
        if ! &rfmkpath ($target);

    foreach my $file (@files)
    {
        return &rfioBridgeFailed ("failed to copy $file to $target", $target)
            if ! &rfcp ("$source/$file", "$target/$file");
    }

    return &rfioBridgeFailed ("failed to copy /dev/null to $target", $target)
        if ! &rfcp ("/dev/null", "$target/go");  # FIXME: go-pending?

    return 1;
}

# Transfer the drop to the next agent
sub relayDrop
{
    my ($self, $drop) = @_;

    # Move to output queue if not done yet
    if (-d "$self->{WORKDIR}/$drop")
    {
        &mv ("$self->{WORKDIR}/$drop", "$self->{OUTDIR}/$drop") || return;
    }

    # Check if we've already successfully copied this one downstream.
    # If so, just nuke it; manual recovery is required to kick the
    # downstream ones forward.
    if (-f "$self->{OUTDIR}/$drop/gone") {
	&rmtree ("$self->{OUTDIR}/$drop");
	return;
    }

    # Clean up our markers
    &rmtree("$self->{OUTDIR}/$drop/go");
    &rmtree("$self->{OUTDIR}/$drop/gone");
    &rmtree("$self->{OUTDIR}/$drop/bad");
    &rmtree("$self->{OUTDIR}/$drop/done");

    # Copy to the next ones.  We want to be careful with the ordering
    # here -- we want to copy the directory exactly once, ever.  So
    # execute in an order that is safe even if we get interrupted.
    if (scalar @{$self->{NEXTDIR}} == 0)
    {
	&rmtree ("$self->{OUTDIR}/$drop");
    }
    elsif (scalar @{$self->{NEXTDIR}} == 1 && $self->{NEXTDIR}[0] !~ /^([a-z]+):/)
    {
	-d "$self->{NEXTDIR}[0]/inbox"
	    || mkdir "$self->{NEXTDIR}[0]/inbox"
	    || -d "$self->{NEXTDIR}[0]/inbox"
	    || return &alert("cannot create $self->{NEXTDIR}[0]/inbox: $!");

	&mv ("$self->{OUTDIR}/$drop", "$self->{NEXTDIR}[0]/inbox/$drop")
	    || return &alert("failed to copy $drop to $self->{NEXTDIR}[0]/$drop: $!");
	&touch ("$self->{NEXTDIR}[0]/inbox/$drop/go")
	    || &alert ("failed to make $self->{NEXTDIR}[0]/inbox/$drop go");
    }
    else
    {
        foreach my $dir (@{$self->{NEXTDIR}})
        {
	    if ($dir =~ /^scp:/) {
		&scpBridgeDrop ("$self->{OUTDIR}/$drop", "$dir/inbox/$drop");
		next;
	    } elsif ($dir =~ /^rfio:/) {
		&rfioBridgeDrop ("$self->{OUTDIR}/$drop", "$dir/inbox/$drop");
		next;
	    }

	    # Local
	    -d "$dir/inbox"
	        || mkdir "$dir/inbox"
	        || -d "$dir/inbox"
	        || return &alert("cannot create $dir/inbox: $!");

	    # Make sure the destination doesn't exist yet.  If it does but
	    # looks like a failed copy, nuke it; otherwise complain and give up.
	    if (-d "$dir/inbox/$drop"
	        && -f "$dir/inbox/$drop/go-pending"
	        && ! -f "$dir/inbox/$drop/go") {
	        &rmtree ("$dir/inbox/$drop")
            } elsif (-d "$dir/inbox/$drop") {
	        return &alert("$dir/inbox/$drop already exists!");
	    }

	    # Copy to the next stage, preserving everything
	    my $status = &runcmd  ("cp", "-Rp", "$self->{OUTDIR}/$drop", "$dir/inbox/$drop");
	    return &alert ("can't copy $drop to $dir/inbox: $status") if $status;

	    # Mark it almost ready to go
	    &touch("$dir/inbox/$drop/go-pending");
        }

        # Now mark myself gone downstream so we won't try copying again
        # (FIXME: error checking?)
        &touch ("$self->{OUTDIR}/$drop/gone");

        # All downstream versions copied safely now.  Now really let them
        # go onwards.  If this fails, it's not fatal because someone can
        # still manually fix them to be in ready state.  We haven't lost
        # anything.  (FIXME: avoidable?)
        foreach my $dir (@{$self->{NEXTDIR}}) {
	    next if $dir =~ /^([a-z]+):/; # FIXME: also handle here?
	    &mv("$dir/inbox/$drop/go-pending", "$dir/inbox/$drop/go");
        }

        # Now junk it here
        &rmtree("$self->{OUTDIR}/$drop");
    }
}

# Check what state the drop is in and indicate if it should be
# processed by agent-specific code.
sub inspectDrop
{
    my ($self, $drop) = @_;

    if (! -d "$self->{WORKDIR}/$drop")
    {
	&alert("$drop is not a pending task");
	return 0;
    }

    if (-f "$self->{WORKDIR}/$drop/bad")
    {
	&alert("$drop marked bad, skipping") if ! exists $self->{BAD}{$drop};
	$self->{BAD}{$drop} = 1;
	return 0;
    }

    if (! -f "$self->{WORKDIR}/$drop/go")
    {
	&alert("$drop is incomplete!");
	return 0;
    }

    if (-f "$self->{WORKDIR}/$drop/done")
    {
	&relayDrop ($self, $drop);
	return 0;
    }

    return 1;
}

# Mark a drop bad.
sub markBad
{
    my ($self, $drop) = @_;
    &touch("$self->{WORKDIR}/$drop/bad");
    &logmsg("stats: $drop @{[&formatElapsedTime($self->{STARTTIME})]} failed");
}

# User hook
sub processDrop
{}

# Manage work queue.  If there are previously pending work, finish
# it, otherwise look for and process new inbox drops.
sub process
{
    my $self = shift;
    $self->init();
    $self->initWorkers();

    while (1)
    {
	my $drop;

	# Check for new inputs.  Move inputs to pending work queue.
	$self->maybeStop();
	foreach $drop ($self->readInbox ())
	{
	    $self->maybeStop();
	    if (! &mv ("$self->{INBOX}/$drop", "$self->{WORKDIR}/$drop"))
	    {
		# Warn and ignore it, it will be returned again next time around
		&alert("failed to move job '$drop' to pending queue: $!");
	    }
	}

	# Check for pending work to do.
	$self->maybeStop();
	my @pending = $self->readPending ();
	my $npending = scalar (@pending);
	foreach $drop (@pending)
	{
	    $self->maybeStop();
	    $self->processDrop ($drop, --$npending);
	}

	# Check for drops waiting for transfer to the next agent.
	$self->maybeStop();
	foreach $drop ($self->readOutbox())
	{
	    $self->maybeStop();
	    $self->relayDrop ($drop);
	}

	# Wait a little while.
	$self->maybeStop();
	$self->idle (@pending);
    }
}

# Wait between scans
sub idle
{
    my ($self, @pending) = @_;
    sleep ($self->{WAITTIME});
}

1;
