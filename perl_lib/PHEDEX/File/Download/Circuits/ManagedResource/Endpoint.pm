package PHEDEX::File::Download::Circuits::ManagedResource::Endpoint;

use Moose;

# Define enums
use Moose::Util::TypeConstraints;
    enum 'LayerType',    [qw(Layer1 Layer2 Layer3)];
no Moose::Util::TypeConstraints;

has 'name'          => (is  => 'ro', isa => 'Str',  required => 1);
has 'address'       => (is  => 'ro', isa => 'Str',  required => 1);
has 'circuitType'   => (is  => 'ro', isa => 'LayerType',  required => 1);

1;