package PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;

use Moose;

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
has 'maxCircuits'   => (is  => 'rw', isa => 'Int', default => 10); # Maximum number of simultaneous circuits at a given time

sub BUILD {
    my $self = shift;
    my $maxBW = min $self->nodeA->maxBandwidth, $self->nodeB->maxBandwidth;
    $self->maxBandwidth($maxBW);
}

sub getName {
    my $self = shift;
    return &getPath($self->nodeA->appName, $self->nodeB->appName, $self->bidirectional);
}

1;