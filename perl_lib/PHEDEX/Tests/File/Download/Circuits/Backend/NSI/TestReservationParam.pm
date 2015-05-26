package PHEDEX::Tests::File::Download::Circuits::Backend::NSI::TestReservationParam;

use strict;
use warnings;

use PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam;
use Test::More;

my $msg = "TestReservationParam";
my $parameters = PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam->new();

is($parameters->bandwidth->arg, "--bw", "$msg: Bandwidth argument was correctly initialised");
is($parameters->bandwidth->value, 1000, "$msg: Bandwidth value was correctly initialised");
is($parameters->description->arg, "--d", "$msg: Description argument was correctly initialised");
ok($parameters->description->value, "$msg: Description value was correctly initialised");
is($parameters->startTime->arg, "--st", "$msg: Start time argument was correctly initialised");
is($parameters->startTime->value, "10 sec", "$msg: Start time value was correctly initialised");
is($parameters->endTime->arg, "--et", "$msg: End time argument was correctly initialised");
is($parameters->endTime->value, "30 min", "$msg: End time value was correctly initialised");
is($parameters->gri->arg, "--g", "$msg: Gri argument was correctly initialised");
is($parameters->gri->value, "PhEDEx-NSI", "$msg: Gri value was correctly initialised");
is($parameters->sourceStp->arg, "--ss", "$msg: Source node argument was correctly initialised");
is($parameters->sourceStp->value, "urn:ogf:network:somenetwork:somestp?vlan=333", "$msg: Source node value was correctly initialised");
is($parameters->destinationStp->arg, "--ds", "$msg: Destination node argument was correctly initialised");
is($parameters->destinationStp->value, "urn:ogf:network:othernetwork:otherstp?vlan=333", "$msg: Destination node value was correctly initialised");

done_testing();

1;