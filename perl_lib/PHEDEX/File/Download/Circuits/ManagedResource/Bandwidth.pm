package PHEDEX::File::Download::Circuits::ManagedResource::Bandwidth;

use Moose;
extends 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource';

use Data::UUID;
use POE;
use POSIX "fmod";
use Switch;

use base 'PHEDEX::Core::Logging';
use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::Core::Timing;

has '+resourceType'     => (is  => 'ro', default => 'Bandwidth', required => 0);
has 'bandwidthStep'     => (is  => 'rw', isa => 'Num', default => 1);
has 'bandwidthMin'      => (is  => 'rw', isa => 'Num', default => 0);
has 'bandwidthMax'      => (is  => 'rw', isa => 'Num', default => 1000);

sub BUILD {
    my $self = shift;
    $self->stateDir($self->stateDir."/".$self->resourceType);
}

# Returns what the save path and save time should be based on current status
sub getSaveParams {
    my $self = shift;
    my ($savePath, $saveTime);
    
    # Bandwidth object will be either put in /offline or /online
    # As opposed to circuit, it's not useful to have a 3rd folder called "/updating"
    # since in the case of BoD the path should remain up even when updating
    # The object goes into the /offline folder if status if offline, or updating and allocated bw = 0
    # Goes into /online for the rest of the cases
    if ($self->status eq 'Offline' ||
        $self->status eq 'Pending' && $self->bandwidthAllocated == 0) {
        $savePath = $self->stateDir.'/offline';
    } else {
        $savePath = $self->stateDir.'/online';
    }
    
    $saveTime = $self->lastStatusChange;
    
    return ($savePath, $saveTime);
}

# Used to register a bandwidth update request 
sub registerUpdateRequest {
    my ($self, $bandwidth, $force) = @_;

    my $msg = 'Bandwidth->registerUpdateRequest';

    if ($self->status eq 'Pending') {
        $self->Logmsg("$msg: Cannot request an update. Update already in progress");
        return ERROR_GENERIC;
    }
    
    return ERROR_GENERIC if $self->validateBandwidth($bandwidth) != OK;
    
    # Check if what we're asking is not already here
    if ($self->bandwidthAllocated > $bandwidth && !$force) {
        $self->Logmsg("$msg: Bandwidth you requested for is already there...");
        return ERROR_GENERIC;
    }
    
    # TODO: Differentiate between ajusting bandwidth up or down
    $self->status('Pending');
    $self->bandwidthRequested($bandwidth);
    $self->Logmsg("$msg: state has been switched to STATUS_UPDATING");

    return OK;
}

# Used to register the new bandwidth following an update request
sub registerUpdateSuccessful {
    my $self = shift;

    my $msg = 'Bandwidth->registerUpdateSuccessful';

    if ($self->status ne 'Pending') {
        $self->Logmsg("$msg: Cannot update status if we're not in updating mode already");
        return ERROR_GENERIC;
    }
    
    $self->bandwidthAllocated($self->bandwidthRequested);
    
    if ($self->bandwidthAllocated == 0) {
        $self->Logmsg("$msg: Effectively turning off the resource (by requesting BW of 0)");
        $self->status('Offline');
    } else {
        $self->Logmsg("$msg: Bandwidth capacity updated");
        $self->status('Online');
    }
    
    return OK;
}

# TODO: Assuming that when an update request fails, it just means it gets denied, thus
# previous reserved bandwidth is still in place. Need to update if this is not the case
# TODO: Do we need to remember this failure? If we do, also add support for storing reason  
sub registerUpdateFailed {
    my $self = shift;

    my $msg = 'Bandwidth->registerUpdateFailed';

    if ($self->status ne 'Pending') {
        $self->Logmsg("$msg: Cannot update status if we're not in updating mode already");
        return ERROR_GENERIC;
    }

    $self->bandwidthAllocated == 0 ? $self->status('Offline') : $self->status('Online');

    return OK;
}

sub validateBandwidth {
    my ($self, $bandwidth) = @_;

    my $msg = 'Bandwidth->validateBandwidth';

    # Check that the bandwidth was correctly specified
    if (! defined $bandwidth || 
        $bandwidth < $self->bandwidthMin || $bandwidth > $self->bandwidthMax ||
        fmod($bandwidth, $self->bandwidthStep) != 0) {
        $self->Logmsg("$msg: Invalid bandwidth request");
        return ERROR_GENERIC;
    } else {
        return OK;
    }
}

1;
