package PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;

use Moose;

use base 'PHEDEX::Core::Logging';

use PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition;
use PHEDEX::File::Download::Circuits::ManagedResource::Circuit;

use Moose::Util::TypeConstraints;
    subtype 'ConnectionId', as 'Str', where { my $regex = CONNECTION_ID_REGEX(); $_ =~ /$regex/ }, message { "The value you provided is not a valid connection id"};
no Moose::Util::TypeConstraints;

has 'resource'      => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource');
has 'callback'      => (is  => 'rw', isa => 'Ref');
has 'connectionId'  => (is  => 'rw', isa => 'ConnectionId');
has 'parameters'    => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam', 
                                     default => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam->new() });
has 'stateMachine'  => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine',
                                     default => sub { PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine->new() });

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

# Provides the NSI CLI script which updates the current CLI reservation parameters
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

# Provides the NSI CLI script which terminates the current reservation
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
