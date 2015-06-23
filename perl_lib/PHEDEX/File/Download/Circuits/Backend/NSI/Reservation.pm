=head1 NAME

Backend::NSI::Reservation - Representation of an NSI reservation

=head1 DESCRIPTION

Internal backend representation of an NSI reservation. Each reservation has its own
associated state machine.

=cut
package PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;

use Moose;

use base 'PHEDEX::Core::Logging';

use PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

use Moose::Util::TypeConstraints;
    subtype 'ConnectionId', as 'Str', where { my $regex = CONNECTION_ID_REGEX(); $_ =~ /$regex/ }, message { "The value you provided is not a valid connection id"};
no Moose::Util::TypeConstraints;

=head1 ATTRIBUTES

=over
 
=item C<resource>

NetworkResource object which requested this reservation in the first place

=cut
has 'resource'      => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource');

=item C<connectionId>

Connection ID which is retrieved once we get the first reply from the NSI aggregator

=cut
has 'connectionId'  => (is  => 'rw', isa => 'ConnectionId');

=item C<parameters>

ReservationParam object keeping track of the NSI arguments needed to set up a reservation, and their respective values 

=cut
has 'parameters'    => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam', 
                                     default => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam->new() });
=item C<stateMachine>

State machine of this reservation

=back

=cut
has 'stateMachine'  => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine',
                                     default => sub { PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine->new() });

=head1 METHODS

=over
 
=item C<updateParameters>

Takes the NetworkResource object associated to this reservation request and updates 
the NSI parameters (STPs, start/end times, etc) 

=cut
# Updates the parameters of the reservation based on the circuit requested
sub updateParameters {
    my ($self, $circuit) = @_;
    $self->resource($circuit);
#    $self->parameters->gri->value("No GRI");
    $self->parameters->sourceStp->value($circuit->path->nodeA->netName);
    $self->parameters->destinationStp->value($circuit->path->nodeB->netName);
    # For now ResourceManager cannot provide a start time - it can only provide an end time (based on the lifetime param)
    my $minutes = $circuit->lifetime / 60;
    my $hours = $circuit->lifetime / 3600;
    
    if ($hours > 0) {
        $self->parameters->endTime->value("$hours hours");
    } elsif ($minutes > 0) {
        $self->parameters->endTime->value("$minutes minutes");
    } else {
        $self->parameters->endTime->value($circuit->lifetime." seconds");
    }
}

=item C<getReservationSetterScript>

Provides the NSI CLI script which would be used to set the reservation paramaters in the NSI CLI

=cut
sub getReservationSetterScript {
    my $self = shift;
    my $script = [];

    # Setup the reservation
    foreach my $key (keys %{$self->parameters}) {
        my $arg = $self->parameters->{$key}->arg;
        my $value = $self->parameters->{$key}->value;
        push (@{$script}, "resv set $arg \"$value\"\n");
    }

    return $script;
}

=item C<getOverrideScript>

Provides the NSI CLI script used to override the restrictions to only using commands
according to the current NSI CLI state machine. For example, if multiple reservations
have been made, and we want to terminate one, "nsi terminate" would end the last one 
made. We need to first override and set the correct connection ID, then do terminate.

=cut
sub getOverrideScript {
    my $self = shift;

    if (!defined $self->connectionId) {
        $self->Alert("ConnectionID was not provided");
        return undef;
    }

    my $script = [];
    push (@{$script}, "nsi override\n");
    push (@{$script}, "nsi set --c \"".$self->connectionId."\"\n");
    
    return $script;
}

=item C<getTerminationScript>

Provides the NSI CLI script which terminates the current reservation

=back

=cut
sub getTerminationScript {
    my $self = shift;

    if (! defined $self->connectionId) {
        $self->Alert("ConnectionID was not provided");
        return undef;
    }

    my $script = $self->getOverrideScript();
    push (@{$script}, "nsi terminate\n");

    return $script;
}

1;
