package PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;

use Moose;
use MooseX::Storage;

with Storage('format' => 'JSON', 'io' => 'File');
 
has 'appName'       => (is  => 'ro', isa => 'Str', required => 1);      # Name of the site from the application point of view (ex: T2_ANSE_1)
has 'netName'       => (is  => 'ro', isa => 'Str', required => 1);      # Name of the site from the circuit provider point of view (ex: manlan.internet2.edu:2013:es?vlan=3400)
has 'maxBandwidth'  => (is  => 'rw', isa => 'Int', default => 1000);    # Maximum bandwidth that can be supplied by the node, in Mbps

1;