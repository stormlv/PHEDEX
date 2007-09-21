package PHEDEX::Namespace;

=head1 NAME

PHEDEX::Namespace - implement namespace (size, migration...) checks on SEs

=head1 SYNOPSIS

This wraps the 'stat' commands of different MSS/storage namespaces in a
uniform interface. Adding a new technology should be easy!

=head1 DESCRIPTION

pending...

=head1 EXAMPLES

pending...

=cut

use strict;
use warnings;

use PHEDEX::Core::Catalogue;
use File::Basename;
use base 'Exporter';
our @EXPORT = ();

our %pmap = ( rfio => 'rf',
	      srm  => 'srm',
	      disk => 'unix',
	    );
our %tmap = ( Castor => 'rfio',
	      dCache => 'disk',
	      Disk   => 'disk',
	      DPM    => 'dpns',
	    );
our %stat;

our @attrs = ( ); # qw/ PROXY / );
our (%params,%ro_params);
for my $attr ( @attrs ) { $ro_params{$attr}++; }

%params = (
                STORAGEMAP      => undef,
                TFCPROTOCOL     => 'direct',
                MSSPROTOCOL     => '',
                DESTINATION     => 'any',
		RFIO_USES_RFDIR => 0,
		VERBOSE		=> 0,
		DEBUG		=> 0,
	  );

sub _init
{
  my $self = shift;
  my %h = @_;

  if ( $h{protocol} ) { $self->protocol( delete $h{protocol} ); }
  map { $self->{$_} = $h{$_} || $params{$_}; } keys %params;

  return $self;
}

sub new
{
  my $proto  = shift;
  my $class  = ref($proto) || $proto;
  my $parent = ref($proto) && $proto;
  my $self = {  };
  bless($self, $class);
  $self->_init(@_);
}

sub AUTOLOAD
{
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;

# Setters and getters...
  return $self->{$attr} if $ro_params{$attr};
  if ( exists $params{$attr} )
  {
    $self->{$attr} = shift if @_;
    return $self->{$attr};
  }

  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods

  if ( $attr !~ m%^$self->{prefix}% ) { die "Unknown method: $attr\n"; }
  $_ = $self->{prefix} . $attr;
  no strict 'refs';
  return $self->$_(@_);
}

sub protocol
{
  my ($self,$protocol) = @_;

  if ( $protocol )
  {
    die "protocol '$protocol' not known. Only know about '" . join("', '", keys %pmap) . "'\n" unless defined $pmap{$protocol};
    $self->{prefix}   = $pmap{$protocol};
    $self->{protocol} = $protocol;
    print "Using TFC protocol $protocol\n";
    if ( $protocol eq 'srm' )
    {
#      open VPI, "voms-proxy-info -timeleft 2>/dev/null |" or die "voms-proxy-info: $!\n";
#      while ( <VPI> )
#      {
#        $self->{DEBUG} && print "voms-proxy-info: $_";
#        next unless m%^(\d+)$%;
#        $self->{PROXY} = $1;
#        $self->{PROXY_EXPIRES} = $self->{PROXY} + time();
#        $self->{PROXY_REPORTED} = 0;
#      }
#      close VPI; # or die "close voms-proxy-info: $!\n";
#      die "no valid proxy? Giving up...\n" unless 
#	( defined($self->{PROXY}) and $self->{PROXY} > 0 );
    }
  }

  return $self->{protocol};
}

sub technology
{
  my ($self,$technology) = @_;
  return $self->protocol() unless defined $technology;
  die "technology '$technology' not known. Only know about '" . join("', '", keys %tmap) . "'\n" unless defined $tmap{$technology};
  print "Using MSS technology $technology\n";
  return $self->protocol($tmap{$technology});
}

sub lfn2pfn
{ 
  my $self = shift;
  my $lfn = shift;
  my $pfn = pfnLookup(  $lfn,
                        $self->{TFCPROTOCOL},
                        $self->{DESTINATION},
                        $self->{STORAGEMAP}
                     );
  return $pfn;
}

sub stat
{
  my $self = shift;
  my ($cmd,$pfn,%r);
  $cmd = shift;

  foreach my $pfn ( @_ )
  {
    next if exists $stat{$pfn};
    open STAT, "$cmd $pfn 2>&1 |" or die "$cmd $pfn: $!\n";
    while ( <STAT> ) { $stat{$pfn}{RAW} .= $_; }
    close STAT; # or die "close $cmd $pfn: $!\n";
  }

  foreach $pfn ( @_ ) { $r{$pfn} = $stat{$pfn}; }
  return \%r;
}

sub _stat
{
  my $self = shift;
  my $cmd = shift;

  my ($pfn,%r);
  foreach my $pfn ( @_ )
  {
    die "Something wrong in _stat...\n" unless defined $pfn;
    next if exists $stat{$pfn};
    open STAT, "$cmd $pfn 2>&1 |" or die "$cmd $pfn: $!\n";
    while ( <STAT> ) { $stat{$pfn}{RAW} .= $_; }
    close STAT; # or die "close $cmd $pfn: $!\n";
  }

  foreach $pfn ( @_ ) { $r{$pfn} = $stat{$pfn}; }
  return \%r;
}

sub Raw
{
  my $self = shift;
  my $pfn  = shift;
  return $stat{$pfn}{RAW};
}

sub stat_key
{
  my $self = shift;
  my $key  = shift;
  my $r;

  $_ = $self->{prefix} . 'stat';
  {
    no strict 'refs';
    $r = $self->$_(@_);
  }
  if ( scalar @_ == 1 ) { return $r->{$_[0]}{$key}; }
  my %q;
  map { $q{$_} = $r->{$_}{$key} } keys %{$r};
}

sub statsize
{
  my $self = shift;
  return $self->stat_key('Size',@_);
}

sub statmode
{
  my $self = shift;
  return $self->stat_key('Migrated',@_);
}

#-----------------------
# protocol-specific bits

# RFIO
sub rfstat
{
  my $self = shift;
  my ($pfn,$r,$cmd);
  $cmd = 'nsls -l';
  if ( $self->{RFIO_USES_RFDIR} ) { $cmd = 'rfdir'; }

  $self->_stat($cmd,@_);
  foreach my $pfn ( @_ )
  {
    next if exists $stat{$pfn}{Size};
    if ( $self->{VERBOSE} >= 3 ) { print "$cmd $pfn...\n"; }
    foreach ( split("\n", $stat{$pfn}{RAW}) )
    {
      chomp;
      m%^([-dm])\S+\s+\S+\s+\S+\s+\S+\s+(\d+).*$pfn$% or next;
      $stat{$pfn}{Size} = $2;
      my $m = $1;
      $stat{$pfn}{Migrated} = ( $m eq 'm' ? 1 : 0 );
    }
  }

  map { $r->{$_} = $stat{$_} } @_;
  return $r;
}

#-----------------------
# SRM
sub srmstat
{
  my $self = shift;
  my ($pfn,$r,$cmd);
  $cmd = 'srm-get-metadata';

#  my $expires = $self->{PROXY_EXPIRES} - time();
#  my $last = time - $self->{PROXY_REPORTED};
#  die "Proxy has expired!\n" if $expires <= 0;
#  if ( $expires < 3600 && $last > 60 )
#  {
#    $self->{PROXY_REPORTED} = time;
#    print scalar localtime," : Proxy expires in $expires seconds\n";
#  }

  $self->_stat($cmd,@_);
  foreach my $pfn ( @_ )
  {
    next if exists $stat{$pfn}{Size};
    if ( $self->{VERBOSE} >= 3 ) { print "$cmd $pfn...\n"; }
    foreach ( split("\n", $stat{$pfn}{RAW}) )
    {
      chomp;
      if ( m%^\s+size\s*:\s*(\d+)% ) { $stat{$pfn}{Size} = $1; }
    }
  }

  map { $r->{$_} = $stat{$_} } @_;
  return $r;
}

#-----------------------
# DPM
sub dpmstat
{
  my $self = shift;
  my ($pfn,$r,$cmd);
  $cmd = 'dpns-ls';

  $self->_stat($cmd,@_);
  foreach my $pfn ( @_ )
  {
    next if exists $stat{$pfn}{Size};
    if ( $self->{VERBOSE} >= 3 ) { print "$cmd $pfn...\n"; }
    foreach ( split("\n", $stat{$pfn}{RAW}) )
    {
      chomp;
      m%^([-dm])\S+\s+\S+\s+\S+\s+\S+\s+(\d+).*$pfn$% or next;
      $stat{$pfn}{Size} = $2;
    }
  }

  map { $r->{$_} = $stat{$_} } @_;
  return $r;
}

#-----------------------
# DCACHE / Disk
sub unixstat
{
  my $self = shift;
  my ($pfn,$r,$cmd);
  $cmd = 'ls -ls';

  $self->_stat($cmd,@_);
  foreach my $pfn ( @_ )
  {
    next if exists $stat{$pfn}{Size};
    if ( $self->{VERBOSE} >= 3 ) { print "$cmd $pfn...\n"; }
    foreach ( split("\n", $stat{$pfn}{RAW}) )
    {
      chomp;
      my $bpfn = basename $pfn;
      m%^\s*\d+\s+([-dm])\S+\s+\S+\s+\S+\s+\S+\s+(\d+).*$bpfn$% or next;
      $stat{$pfn}{Size} = $2;
    }
  }

  map { $r->{$_} = $stat{$_} } @_;
  return $r;
}

1;
