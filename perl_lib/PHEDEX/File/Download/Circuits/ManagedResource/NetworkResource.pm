package PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

use Moose;
use MooseX::Storage;

# Chose YAML to as a format since it seems to be more human readable than the JSON output
# that MooseX::Storage puts out (which cannot be configured)
with Storage('format' => 'YAML', 'io' => 'File');

use base 'PHEDEX::Core::Logging';

use Data::UUID;
use Switch;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Common::EnumDefinitions;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;

# Define enums
use Moose::Util::TypeConstraints;
    enum 'ScopeType',       SCOPE_TYPES;
    enum 'StatusType',      STATUS_TYPES;
    subtype 'PositiveNum', as 'Num', where { $_ >= 0 }, message { "The number you provided, $_, was not a positive number" };
no Moose::Util::TypeConstraints;

# TODO: Check if it's possible to add a callback attribute.
# It would be useful to notify the RM when the state changes
# Remains to be checked how it handles serialisation ...
has 'backendName'           => (is  => 'ro', isa => 'Str', required => 1);
has 'bandwidthAllocated'    => (is  => 'rw', isa => 'PositiveNum', default => 0);
has 'bandwidthRequested'    => (is  => 'rw', isa => 'PositiveNum', default => 0);
has 'bandwidthUsed'         => (is  => 'rw', isa => 'PositiveNum', default => 0);
has 'establishedTime'       => (is  => 'rw', isa => 'PositiveNum');
has 'failures'              => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Common::Failure]', 
                                traits  => ['Array'], 
                                handles => {addFailure      => 'push', 
                                            getFailure      => 'get',
                                            getAllFailures  => 'elements',
                                            hasFailures     => 'is_empty',
                                            countFailures   => 'count',
                                            filterFailures  => 'grep'}, # grep requires a subroutine which implements the matching logic (elements available to sub in $_);
                                trigger =>  \&_healthCheck);
has 'id'                    => (is  => 'ro', isa => 'Str', 
                                default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()); });
has 'lifetime'              => (is  => 'rw', isa => 'PositiveNum', default => 6*HOUR);
has 'maxFailureCount'       => (is  => 'rw', isa => 'PositiveNum', default => 1000);
has 'path'                  => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::Core::Path', required => 1);
has 'physicalId'            => (is  => 'rw', isa => 'Str');
has 'requestedTime'         => (is  => 'rw', isa => 'PositiveNum');
has 'scope'                 => (is  => 'rw', isa => 'ScopeType',    default => 'Generic');
has 'status'                => (is  => 'rw', isa => 'StatusType',   default => 'Created');
has 'stateDir'              => (is  => 'rw', isa => 'Str',          default => '/tmp/resources');
has 'lastStatusChange'      => (is  => 'rw', isa => 'PositiveNum',  default => &mytimeofday());
has 'verbose'               => (is  => 'rw', isa => 'Bool',         default => 1);

sub _healthCheck {
    my ($self, $newState, $oldState) = @_;
    my $msg = "NetworkResource->healthCheck";
    if ($self->countFailures > $self->maxFailureCount) {
        $self->Logmsg("$msg: Maximum failure count has been exceeded. Taking the circuit offline");
        $self->setStatus("Offline");
    }
}

sub setStatus {
    my ($self, $status, $arg0, $arg1) = @_; 
    my $msg = "NetworkResource->setStatus";
    
    # Arg0 = {
    #         - bwRequested (if requesting or updating)
    #         - bwAllocated (if switching to online)
    #        }
    # Arg1 = physicalID (only used when switching to online)
    
    if (! defined $status) {
        $self->Logmsg("$msg: Invalid arguments provided");
        return;
    } 

    # We could replace this by a state machine like in the case of Reservation (NSI), but 
    # this one is quite trivial, with having only a few states. It can be handled in a simple switch
    my $timeNow = &mytimeofday();
    switch($status) {
        case ['Requesting', 'Updating'] {
            if ($status eq 'Requesting' && $self->status ne 'Created' ||
                $status eq 'Updating' && $self->status ne 'Online' ||
                ! defined $arg0) {
                $self->Logmsg("$msg: Cannot switch to $status");
                return;
            }
            $self->requestedTime($timeNow);
            $self->bandwidthRequested($arg0);
        };
        case 'Online' {
            if ($self->status ne 'Requesting' && $self->status ne 'Updating' && ! &checkArguments(@_) ||
                ! defined $arg0 || ! defined $arg1) {
                $self->Logmsg("$msg: Cannot switch to $status");
                return;
            }
            $self->establishedTime($timeNow);
            $self->bandwidthAllocated($arg0);
            $self->physicalId($arg1);
        }
        case 'Offline' {
            if ($self->status eq 'Created' || $self->status eq 'Offline') {
                $self->Logmsg("$msg: Cannot switch to $status");
                return;
            }
        }
        default {
            $self->Logmsg("$msg: $status is unkown");
            return;
        }
    };
    
    $self->status($status);
    $self->lastStatusChange($timeNow);
    return $status;
}

# Returns the expiration time if the resource is Online
sub getExpirationTime {
    my $self = shift;

    if ($self->status ne 'Online' || ! defined $self->establishedTime) {
        $self->Logmsg("NetworkResource->getExpirationTime: Resource is not yet online, so to established time has been set");
        return undef;
    }

    return $self->establishedTime + $self->lifetime;
}

# Checks to see if the resource expired or not (if LIFETIME was defined)
sub isExpired {
    my $self = shift;
    my $expiration = $self->getExpirationTime;
    if (! defined $expiration) {
        $self->Logmsg("NetworkResource->isExpired: Resource is not yet online, so to established time has been set");
        return 0;
    };
    return $expiration <= &mytimeofday(); 
}

sub getLinkName {
    my $self = shift;
    return $self->path->getName;
}

# Returns the location where this resource will be saved
# eg. /tmp/Online
sub getSaveLocation {
    my $self = shift;
    return $self->stateDir.'/'.$self->status;
}

# Returns the filename of the current resource
# It has the following form: 
#       NodeA-[to]-NodeB-PartialUID-Date-Time
# T2_ANSE_Amsterdam-to-T2_ANSE_Geneva-xyzxyzx-20140427-10:00:00, for a unidirectional link
# T2_ANSE_Amsterdam-T2_ANSE_Geneva-xyzxyzx-20140427-10:00:00, for a bidirectional link
sub getSaveFilename {
    my $self = shift;

    my $partialID = substr($self->id, 1, 7);
    my $formattedTime = &getFormattedTime($self->lastStatusChange);
    my $link = $self->getLinkName();
    my $fileName = $link."-$partialID-".$formattedTime;
    
    return $fileName;
}

# Simply returns the full save path (folder/filename)
sub getFullSavePath {
    my $self = shift;
    return $self->getSaveLocation()."/".$self->getSaveFilename();
}

# Saves the current state of the resource
sub saveState { 
    my ($self, $overrideLocation) = @_;
    my $msg = 'NetworkResource->saveState';

    # Check if state folder existed and attempt to create if it didn't
    my $location = defined $overrideLocation ? $overrideLocation : $self->getSaveLocation();
    if (!-d $location) {
        File::Path::make_path($location, {error => \my $err});
        if (@$err) {
            $self->Logmsg("$msg: State folder did not exist and we were unable to create it");
            return ERROR_PATH_INVALID;
        }
    }
    
    # Attempt to save the object
    my $fullPath = $location."/".$self->getSaveFilename();
    $self->store($fullPath);
}

# Attempts to remove the state file associated with this circuit
sub removeState {
    my $self = shift;

    my $msg = 'NetworkResource->removeState';
    my $location = $self->getSaveLocation();
    my $fullPath = $self->getFullSavePath();

    if (!-d $location || !-e $fullPath) {
        $self->Logmsg("$msg: There's nothing to remove from the state folders");
        return ERROR_GENERIC;
    }

    return !(unlink $fullPath) ? ERROR_GENERIC : OK;
}

sub TO_JSON {
    return { %{ shift() } };
}

1;