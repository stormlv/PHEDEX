=head1 NAME

Backend::NSI::Action - Holds attributes for actions queued by the NSI backend

=head1 DESCRIPTION

The NSI CLI doesn't handle multitasking well and because of this, all the commmands
have to be queued - these commands are what I call actions.

At creation, an action requires:

    type: possible action types: REQUEST, UPDATE, TEARDOWN
    
    resource: the resource for which this action is being executed
    
    callback: the callback which will be triggered when the action finishes

This object also holds the NSI reservation object (associated with the NetworkResource object 
which requested it)

=cut
package PHEDEX::File::Download::Circuits::Backend::NSI::Action;

use Moose;

use PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

has 'id'            => (is  => 'ro', isa => 'Str', default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()); });
has 'type'          => (is  => 'rw', isa => 'Str', required => 1);
has 'resource'      => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource', required => 1);
has 'callback'      => (is  => 'rw', isa => 'Ref', required => 1);
has 'reservation'   => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::Reservation');

1;