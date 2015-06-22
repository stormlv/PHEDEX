=head1 NAME

ManagedResource::Core::Node - Object abstraction for nodes

=head1 DESCRIPTION

When constructing it, it requires:

    appName: Name of the site from the application point of view (ex: T2_ANSE_1)
    
    netName: Name of the site from the circuit provider point of view (ex: manlan.internet2.edu:2013:es?vlan=3400)

Additionally, the maximum bandwidth (in Mbps) supported by the node, can be supplied as well

=cut
package PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;

use Moose;
use MooseX::Storage;

with Storage('format' => 'JSON', 'io' => 'File');
 
has 'appName'       => (is  => 'ro', isa => 'Str', required => 1);
has 'netName'       => (is  => 'ro', isa => 'Str', required => 1); 
has 'maxBandwidth'  => (is  => 'rw', isa => 'Int', default => 1000);

1;