package PHEDEX::File::Download::Circuits::ManagedResource::Circuit;

use Moose;
use Moose::Util::TypeConstraints;

extends 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource';

use base 'PHEDEX::Core::Logging';
use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Common::Failure;

use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::Helpers::Utils::UtilsConstants;

use Switch;

subtype 'IP', as 'Str', where {&determineAddressType($_) ne ADDRESS_INVALID}, message { "The value you provided is not a valid hostname or IP(v4/v6)"};

has '+resourceType'     => (is  => 'ro', default => 'Circuit', required => 0);
has 'establishedTime'   => (is  => 'rw', isa => 'Num');
has 'ipA'               => (is  => 'rw', isa => 'IP');
has 'ipB'               => (is  => 'rw', isa => 'IP');
has 'lifetime'          => (is  => 'rw', isa => 'Num', default => 6*HOUR);
has 'requestedTime'     => (is  => 'rw', isa => 'Num');
has 'requestTimeout'    => (is  => 'rw', isa => 'Int', default => 5*MINUTE);

sub BUILD {
    my $self = shift;
    $self->stateDir($self->stateDir."/".$self->resourceType);
}

# Returns the expiration time if the circuit is Online
sub getExpirationTime {
    my $self = shift;
    return $self->status eq 'Online' && defined $self->establishedTime ? $self->establishedTime + $self->lifetime : undef;
}

# Checks to see if the circuit expired or not (if LIFETIME was defined)
sub isExpired {
    my $self = shift;
    my $expiration = $self->getExpirationTime;
    return defined $expiration && $expiration < &mytimeofday() ? 1 : 0;
}

# Method used to switch state from OFFLINE to REQUESTING
# Backend has to be provided when requesting circuits
sub registerRequest {
    my ($self, $lifetime, $bandwidth) = @_;

    my $msg = 'Circuit->registerRequest';

    # Cannot change status to STATUS_UPDATING if
    #   - Circuit is not previously initialised
    #   - The status is not prior STATUS_OFFLINE
    if (!defined $self->id || $self->status ne 'Offline') {
        $self->Logmsg("$msg: Cannot change status to pending");
        return ERROR_GENERIC;
    }

    $self->status('Pending');
    $self->requestedTime(&mytimeofday());
    
    # These two parameters can be undef
    $self->lifetime($lifetime) if defined $lifetime;
    $self->bandwidthRequested($bandwidth) if defined $bandwidth;

    $self->Logmsg("$msg: state has been switched to STATUS_UPDATING");

    return OK;
}

# Method used to switch state from REQUESTING to ONLINE
sub registerEstablished {
    my ($self, $ipA, $ipB, $bandwidth) = @_;

    my $msg = 'Circuit->registerEstablished';

    # Cannot change status to STATUS_ONLINE if
    #   - The status is not prior STATUS_UPDATING
    #   - both $ipA and $ipB are not valid addresses
    if ($self->status ne 'Pending' ||
        determineAddressType($ipA) eq ADDRESS_INVALID ||
        determineAddressType($ipB) eq ADDRESS_INVALID) {
        $self->Logmsg("$msg: Cannot change status to STATUS_ONLINE");
        return ERROR_GENERIC;
    }

    $self->status('Online');
    $self->establishedTime(&mytimeofday());
    $self->ipA($ipA);
    $self->ipB($ipB);

    # These two can also be undef
    $self->bandwidthAllocated($bandwidth) if defined $bandwidth;

    $self->Logmsg("$msg: state has been switched to STATUS_ONLINE");
    return OK;
}

# Method used to switch state from ONLINE to OFFLINE
sub registerTakeDown {
    my $self = shift;

    my $msg = 'Circuit->registerTakeDown';

    if ($self->status ne 'Online') {
        $self->Logmsg("$msg: Cannot change status to STATUS_OFFLINE");
        return ERROR_GENERIC;
    }

    $self->status('Offline');

    $self->Logmsg("$msg: state has been switched to STATUS_OFFLINE");
    return OK;
}

# All failures should be tracked in {FAILURES} by (timestamp and reason)

# Method used to switch state from REQUESTING to OFFLINE
# It is recommended to provide a reason why this request failed
sub registerRequestFailure {
    my ($self, $reason) = @_;

    my $msg = 'Circuit->registerRequestFailure';

    if ($self->status ne 'Pending') {
        $self->Logmsg("$msg: Cannot register a request failure for a circuit not STATUS_UPDATING");
        return undef;
    }

    $self->status('Offline');

    # Keep track of why the request failed
    my $failure = PHEDEX::File::Download::Circuits::Common::Failure->new(time => $self->lastStatusChange, comment => $reason);
    $self->addFailure($failure);
    
    $self->Logmsg("$msg: Circuit request failure has been registered");

    return $failure;
}

# Method used to keep track of how many transfers failed
# Based on this information CircuitManager might decide to blacklist a circuit
# if too many transfers failed on this particular circuit
sub registerTransferFailure {
    my ($self, $task) = @_;

    # TODO: When registering a failure, it might be nice to also clean up old ones or just "remember the last xxx failures"
    my $msg = 'Circuit->registerTransferFailure';

    if ($self->status ne 'Online') {
        $self->Logmsg("$msg: Cannot register a trasfer failure for a circuit not STATUS_ONLINE");
        return undef;
    }
    
    my $failure = PHEDEX::File::Download::Circuits::Common::Failure->new(time => &mytimeofday(), comment => 'Task failed', faultObject => $task);
    $self->addFailure($failure);
    
    $self->Logmsg("$msg: Circuit transfer failure has been registered") if ($self->verbose);
    return $failure;
}

sub getTransferFailureCount {
    my $self = shift;
    my $failureCount = 0;
    foreach my $failure ($self->getAllFailures()) {
        $failureCount ++ if ($failure->comment eq 'Task failed');
    }
    return 0;
}

# Returns what the save path and save time should be based on current status
sub getSaveParams {
    my $self = shift;
    my ($savePath, $saveTime);

    $savePath = $self->stateDir.'/'.$self->status;
    $saveTime = $self->lastStatusChange;

    return ($savePath, $saveTime);
}

1;