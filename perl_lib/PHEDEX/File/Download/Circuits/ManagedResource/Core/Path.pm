=head1 NAME

ManagedResource::Core::Path - Object abstraction for a path

=head1 DESCRIPTION

When constructing it, it requires:

    nodeA and NodeB: Node objects for each of the end points of a path
    
    type: type of patch which is supported via circuits (Layer 1, Layer 2, Layer 3)

After construction the maxBandwidth is set as well

=cut

package PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;

use Moose;
use MooseX::Storage;

with Storage('format' => 'JSON', 'io' => 'File');

use PHEDEX::File::Download::Circuits::Common::EnumDefinitions;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;

use List::Util qw(min);

use Moose::Util::TypeConstraints;
    enum 'LayerType',    LAYER_TYPES;
no Moose::Util::TypeConstraints;

has 'bidirectional' => (is  => 'rw', isa => 'Bool', default => 1);
has 'nodeA'         => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::Core::Node', required => 1);
has 'nodeB'         => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::Core::Node', required => 1);
has 'type'          => (is  => 'ro', isa => 'LayerType', required => 1);
has 'maxBandwidth'  => (is  => 'rw', isa => 'Int');
has 'maxCircuits'   => (is  => 'rw', isa => 'Int', default => 10);

sub BUILD {
    my $self = shift;
    return if defined $self->maxBandwidth;
    my $maxBW = min $self->nodeA->maxBandwidth, $self->nodeB->maxBandwidth;
    $self->maxBandwidth($maxBW);
}

=head1 METHODS

=over
 
=item C<getName>

Returns the path names, by concatenating the app names of each of the nodes.
If the link is bidirectional, the appName, will be sorted alphabetically.

=back

=cut

sub getName {
    my $self = shift;
    return &getPath($self->nodeA->appName, $self->nodeB->appName, $self->bidirectional);
}

1;