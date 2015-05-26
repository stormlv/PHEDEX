package PHEDEX::Tests::File::Download::Circuits::Backend::NSI::TestReservation;

use strict;
use warnings;

use base 'PHEDEX::Core::Logging';

use PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;
use PHEDEX::File::Download::Circuits::ManagedResource::Circuit;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;

use Test::More;

# Create the path with its respective two nodes
my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'NodeA', netName => 'STP1', maxBandwidth => 111);
my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'NodeB', netName => 'STP2', maxBandwidth => 222);
my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');

# Create the circuit and reservation, then update the reservation's parameters based on the provided circuit
my $circuit = PHEDEX::File::Download::Circuits::ManagedResource::Circuit->new(backendType => 'Dummy', path => $path);
my $reservation = PHEDEX::File::Download::Circuits::Backend::NSI::Reservation->new();
$reservation->updateParameters($circuit);

my $reservationScript = $reservation->getReservationSetterScript();
my $scriptHash;

# Yes, this is an ugly way to test it
foreach my $line (@{$reservationScript}) { $scriptHash->{$line} = 1; }

ok($scriptHash->{'resv set --bw "1000"'."\n"}, "BW correctly set");
ok($scriptHash->{'resv set --ss "STP1"'."\n"}, "Node 1 correctly set");
ok($scriptHash->{'resv set --ds "STP2"'."\n"}, "Node 2 correctly set");
ok($scriptHash->{'resv set --et "21600 sec"'."\n"}, "Lifetime correctly set");
ok($scriptHash->{'resv set --st "10 sec"'."\n"}, "Start time correctly set");

# Check the getTermination script method
my $failedTerminationScript = $reservation->getTerminationScript();
ok(! defined $failedTerminationScript, "Cannot generate termmination script without knowing the reservation connection id");
 
$reservation->connectionId("d005b619-16be-4312-82bf-4960ebdc6326");
my $terminationScript = $reservation->getTerminationScript();
is($terminationScript->[0], "nsi override\n", "First line of termination script looks ok");
is($terminationScript->[1], "nsi set --c \"d005b619-16be-4312-82bf-4960ebdc6326\"\n", "Second line of termination script looks ok");
is($terminationScript->[2], "nsi terminate\n", "Third line of termination script looks ok");

done_testing();

1;