package PHEDEX::Web::API::Inject;
use warnings;
use strict;
use PHEDEX::Core::XML;
use PHEDEX::Core::Inject;
use PHEDEX::Web::Util;

=pod

=head1 NAME

PHEDEX::Web::API::Inject - Inject data into TMDB

=head2 inject

Inject data into TMDB, returning the statistics
on how many files, blocks, and datasets were injected etc.

=head3 options

 node		Required node-name as injection site.
 data		XML structure representing the data to be injected. See
		PHEDEX::Core::XML

 verbose	be verbose
 strict		throw an error if it can't insert the data exactly as
		requested. Otherwise simply return the statistics. The
		default is to be strict, you can turn it off with 'nostrict'.

=head3 return value

returns a hash with keys for the data, the node, the node-id, and the injection
statistics. The statistics is also a hash, with keys for:

 new datasets		number of new datasets created
 new blocks		number of new blocks created
 new files		number of new files created
 closed datasets	number of closed datasets injected
 closed blocks		number of closed blocks injected

If 'nostrict' is specified, attempting to re-insert already-inserted data will
not give an error, but all the stats values will be zero.

=cut

sub duration  { return 0; }
sub need_auth { return 1; }
sub invoke { return inject(@_); }
sub inject
{
  my ($core,%args) = @_;
  &checkRequired(\%args, 'node');

  my ($auth,$node,$nodeid,$result,$stats,$verbose,$strict);
  $core->{SECMOD}->reqAuthnCert();
  $auth = $core->getAuth('datasvc_inject');
  $node = $args{node};

  $nodeid = $auth->{NODES}->{$node} || 0;
  die("You are not authorised to inject data to node $node") unless $nodeid;
  $result = PHEDEX::Core::XML::parseData( XML => $args{data} );

  $verbose = defined $args{verbose} ? $args{verbose} : 0;
  $strict  = defined $args{strict}  ? $args{strict}  : 1;

  eval
  {
    $stats = PHEDEX::Core::Inject::injectData ($core, $result, $nodeid,
				    		VERBOSE => $verbose,
				    		STRICT  => $strict);
  };
  if ( $@ )
  {
    $core->{DBH}->rollback(); # Processes seem to hang without this!
    die $@;
  }

  # determine if we commit
  my $commit = 0;
  if (%$stats) {
      $commit = 1;
  } else {
      die "no injection was done\n";
      $core->{DBH}->rollback();
  }
  $commit = 0 if $args{dummy};
  $commit ? $core->{DBH}->commit() : $core->{DBH}->rollback();

  return { injected => { stats => $stats } };
}

1;
