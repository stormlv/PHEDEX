package PHEDEX::Tests::File::Download::Circuits::ManagedResource::Core::TestPath;

use strict;
use warnings;

use Test::More;

use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;

my $msg = "TestPath";

my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(siteName => 'nodeA', endpointName => 'STP1', maxBandwidth => 111);
my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(siteName => 'nodeB', endpointName => 'STP2', maxBandwidth => 222);
my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');

is($path->maxBandwidth, 111, "$msg: Got the correct bandwidth");
is($path->getName, 'nodeA-nodeB', "$msg: Got the correct name");

done_testing();

1;
