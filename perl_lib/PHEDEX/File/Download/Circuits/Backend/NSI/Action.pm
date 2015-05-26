package PHEDEX::File::Download::Circuits::Backend::NSI::Action;

use Moose;

use PHEDEX::File::Download::Circuits::ManagedResource::Circuit;
use PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;

has 'id'            => (is  => 'rw', isa => 'Str', required => 1);
has 'type'          => (is  => 'rw', isa => 'Str', required => 1);
has 'circuit'       => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::Circuit', required => 1);
has 'callback'      => (is  => 'rw', isa => 'Ref', required => 1);
has 'reservation'   => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::Reservation');

1;