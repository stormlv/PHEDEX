package PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;

use Moose;

has 'siteName'      => (is  => 'ro', isa => 'Str', required => 1);      # Name of the site from the application point of view (PhEDEx, PanDA, etc.)
has 'endpointName'  => (is  => 'ro', isa => 'Str', required => 1);      # Name of the site from the circuit provider point of view (STP names, OSCARS IDCs, etc.)
has 'maxBandwidth'  => (is  => 'rw', isa => 'Int', default => 1000);    # Maximum bandwidth that can be supplied by the node, in Mbps

1;