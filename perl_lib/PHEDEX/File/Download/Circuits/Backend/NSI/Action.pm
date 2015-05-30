package PHEDEX::File::Download::Circuits::Backend::NSI::Action;

use Moose;

use PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

has 'id'            => (is  => 'rw', isa => 'Str', required => 1);
has 'type'          => (is  => 'rw', isa => 'Str', required => 1);
has 'resource'      => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource', required => 1);
has 'callback'      => (is  => 'rw', isa => 'Ref', required => 1);
has 'reservation'   => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::Reservation');

1;