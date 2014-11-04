package PHEDEX::File::Download::Circuits::ManagedResource::Circuit;

use strict;
use warnings;

use base 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource', 'PHEDEX::Core::Logging';
use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;
use PHEDEX::File::Download::Circuits::TFCUtils;

use Switch;
use Scalar::Util qw(blessed);

# Use registerRequest, registerEstablished, registerTakeDown and registerRequestFailure
# in order to ensure a consistent state change throughout the object's lifetime
# Do not modify these parameters directly! (unless you know what you're doing - then it's ok)

# Ideas for later on - unused parameters for now:
# - SCOPE: This can be used if we'd have multiple circuits per link
#       and we'd want a way to discriminate againts using some circuits over others
#       (like possibly using different circuits for different protocols, etc.)
# - BANDWIDTH_REQUESTED and BANDWIDTH_USED: could be used to track the performance of the circuit.
sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my %params = (
            SCOPE                   =>  'GENERIC',
            
            LIFETIME                =>  undef,
            REQUEST_TIME            =>  undef,          # Time the circuit was requested
            ESTABLISHED_TIME        =>  undef,          # Time the circuit was established
            IP_B                    =>  undef,
            IP_A                    =>  undef,

            FAILURES                =>  {
                                            CIRCUIT_FAILED_REQUEST          =>      undef,
                                            CIRCUIT_FAILED_TRANSFERS        =>      [],
                                        },

            # General parameters
            CIRCUIT_REQUEST_TIMEOUT       => 5*MINUTE,         # in seconds
            CIRCUIT_DEFAULT_LIFETIME      => 5*HOUR,      # in seconds

            # Performance related parameters
            BANDWIDTH_REQUESTED     =>  undef,              # Bandwidth we requested
    );
				
    my %args = (@_);

    #   use 'defined' instead of testing on value to allow for arguments which are set to zero.
    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = $class->SUPER::new(%args);

    bless $self, $class;

    return $self;
}

sub initResource {
    my ($self, $backend, $nodeA, $nodeB, $bidirectional) = @_;
    
    # Do our own initialisation
    $self->{STATE_DIR}.="/circuits";
    $self->{STATUS} = STATUS_CIRCUIT_OFFLINE;
    
    return $self->SUPER::initResource($backend, CIRCUIT, $nodeA, $nodeB, $bidirectional);
}

# Returns the expiration time if LIFETIME was defined; undef otherwise
sub getExpirationTime {
    my $self = shift;
    return $self->{STATUS} == STATUS_CIRCUIT_ONLINE &&
           defined $self->{LIFETIME} ? $self->{ESTABLISHED_TIME} + $self->{LIFETIME} : undef;
}

# Checks to see if the circuit expired or not (if LIFETIME was defined)
sub isExpired {
    my $self = shift;
    my $expiration = $self->getExpirationTime();
    return defined $expiration && $expiration < &mytimeofday() ? 1 : 0;
}

# Method used to switch state from OFFLINE to REQUESTING
# Backend has to be provided when requesting circuits
sub registerRequest {
    my ($self, $lifetime, $bandwidth) = @_;

    my $msg = 'Circuit->registerRequest';

    # Cannot change status to STATUS_CIRCUIT_REQUESTING if
    #   - Circuit is not previously initialised
    #   - The status is not prior STATUS_CIRCUIT_OFFLINE
    if (!defined $self->{ID} || $self->{STATUS} != STATUS_CIRCUIT_OFFLINE) {
        $self->Logmsg("$msg: Cannot change status to STATUS_CIRCUIT_REQUESTING");
        return ERROR_GENERIC;
    }

    $self->{STATUS} = STATUS_CIRCUIT_REQUESTING;
    $self->{REQUEST_TIME} = &mytimeofday();
    $self->{LAST_STATUS_CHANGE} = $self->{REQUEST_TIME};
    
    # These two parameters can be undef
    $self->{LIFETIME} = $lifetime;
    $self->{BANDWIDTH_REQUESTED} = $bandwidth;

    $self->Logmsg("$msg: state has been switched to STATUS_CIRCUIT_REQUESTING");

    return OK;
}

# Method used to switch state from REQUESTING to ONLINE
sub registerEstablished {
    my ($self, $ipA, $ipB, $bandwidth) = @_;

    my $msg = 'Circuit->registerEstablished';

    # Cannot change status to STATUS_CIRCUIT_ONLINE if
    #   - The status is not prior STATUS_CIRCUIT_REQUESTING
    #   - both $ipA and $ipB are not valid addresses
    if ($self->{STATUS} != STATUS_CIRCUIT_REQUESTING ||
        determineAddressType($ipA) == ADDRESS_INVALID ||
        determineAddressType($ipB) == ADDRESS_INVALID) {
        $self->Logmsg("$msg: Cannot change status to STATUS_CIRCUIT_ONLINE");
        return ERROR_GENERIC;
    }

    $self->{STATUS} = STATUS_CIRCUIT_ONLINE;
    $self->{ESTABLISHED_TIME} = &mytimeofday();
    $self->{LAST_STATUS_CHANGE} = $self->{ESTABLISHED_TIME};
    $self->{IP_A} = $ipA;
    $self->{IP_B} = $ipB;

    # These two can also be undef
    $self->{BANDWIDTH_ALLOCATED} = $bandwidth;

    $self->Logmsg("$msg: state has been switched to STATUS_CIRCUIT_ONLINE");
    return OK;
}

# Method used to switch state from ONLINE to OFFLINE
sub registerTakeDown {
    my $self = shift;

    my $msg = 'Circuit->registerTakeDown';

    if ($self->{STATUS} != STATUS_CIRCUIT_ONLINE) {
        $self->Logmsg("$msg: Cannot change status to STATUS_CIRCUIT_OFFLINE");
        return ERROR_GENERIC;
    }

    $self->{STATUS} = STATUS_CIRCUIT_OFFLINE;
    $self->{LAST_STATUS_CHANGE} = &mytimeofday();

    $self->Logmsg("$msg: state has been switched to STATUS_CIRCUIT_OFFLINE");
    return OK;
}

# All failures should be tracked in {FAILURES} by (timestamp and reason)

# Method used to switch state from REQUESTING to OFFLINE
# It is recommended to provide a reason why this request failed
sub registerRequestFailure {
    my ($self, $reason) = @_;

    my $msg = 'Circuit->registerRequestFailure';

    if ($self->{STATUS} != STATUS_CIRCUIT_REQUESTING) {
        $self->Logmsg("$msg: Cannot register a request failure for a circuit not STATUS_CIRCUIT_REQUESTING");
        return ERROR_GENERIC;
    }

    $self->{STATUS} = STATUS_CIRCUIT_OFFLINE;
    $self->{LAST_STATUS_CHANGE} = &mytimeofday();

    # Keep track of why the request failed
    my $failure = [$self->{LAST_STATUS_CHANGE}, $reason];
    $self->{FAILURES}{CIRCUIT_FAILED_REQUEST} = $failure;

    $self->Logmsg("$msg: Circuit request failure has been registered");

    return OK;
}

# Returns an array with the [time, reason] of the failed request
sub getFailedRequest {
    my $self = shift;
    return $self->{FAILURES}{CIRCUIT_FAILED_REQUEST};
}

# Method used to keep track of how many transfers failed
# Based on this information CircuitManager might decide to blacklist a circuit
# if too many transfers failed on this particular circuit
sub registerTransferFailure {
    my ($self, $task) = @_;

    # TODO: When registering a failure, it might be nice to also clean up old ones or just "remember the last xxx failures"
    my $msg = 'Circuit->registerTransferFailure';

    if ($self->{STATUS} != STATUS_CIRCUIT_ONLINE) {
        $self->Logmsg("$msg: Cannot register a trasfer failure for a circuit not STATUS_CIRCUIT_ONLINE");
        return ERROR_GENERIC;
    }

    my $failure = [&mytimeofday(), $task];
    push(@{$self->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}}, $failure);

    $self->Logmsg("$msg: Circuit transfer failure has been registered") if ($self->{VERBOSE});
    return OK;
}

# Returns an array with all the details regarding the failed transfers
# that occured on this circuit. Each element in the array is in the form
# of [time, reason]
sub getFailedTransfers() {
    my $self = shift;
    return $self->{FAILURES}{CIRCUIT_FAILED_TRANSFERS};
}

# Generates a file name in the form of : FROM_NODE-(to)-TO_NODE-UID-time
# ex. T2_ANSE_Amsterdam-to-T2_ANSE_Geneva-FSDAXSDA-20140427-10:00:00, if link is unidirectional
# or T2_ANSE_Amsterdam-T2_ANSE_Geneva-FSDAXSDA-20140427-10:00:00, if link is bi-directional.
# Returns a save path ({STATE_DIR}/$state) and a file path ({STATE_DIR}/$state/$FROM_NODE-to-$TO_NODE-$time)
# We could also put part of the UUID at the end of the file but for now it is not needed
# unless we would request multiple circuits on the same link *at the same time*...which we don't/won't?
sub getSaveName() {
    my $self = shift;
    my ($filePath, $savePath, $saveTime);
    
    my $msg = "Circuit->getSaveName";
        
    if (! defined $self->{ID}) {
        $self->Logmsg("$msg: Cannot generate a save name for a resource which is not initialised");
        return undef;
    }
    
    switch ($self->{STATUS}) {
        case STATUS_CIRCUIT_REQUESTING {
            $savePath = $self->{STATE_DIR}.'/requested';
            $saveTime = $self->{REQUEST_TIME};
        }
        case STATUS_CIRCUIT_ONLINE {
            $savePath = $self->{STATE_DIR}.'/online';
            $saveTime = $self->{ESTABLISHED_TIME};
        }
        case STATUS_CIRCUIT_OFFLINE {
            $savePath = $self->{STATE_DIR}.'/offline';
            $saveTime = $self->{LAST_STATUS_CHANGE};
        }
    }

    if (!defined $savePath || !defined $saveTime || $saveTime <= 0) {
        $self->Logmsg("$msg: Invalid parameters in generating a circuit file name");
        return undef;
    }

    my $partialID = substr($self->{ID}, 1, 8);
    my $formattedTime = &formattedTime($saveTime);
    $filePath = $savePath.'/'.$self->{NAME}."-$partialID-".$formattedTime;

    return ($savePath, $filePath);
}

1;