=head1 NAME

ManagedResource::NetworkResource - Circuit abstraction object used by the ResourceManager

=head1 DESCRIPTION

This class is mainly used by the ResourceManager and Backend::Core.
It describes a circuit from when it's created up to its termination.

=cut

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

=head1 ATTRIBUTES

=over
 
=item C<backendName>

Backend type which was used to request this circuit. It's a required attribute

=cut 
has 'backendName'           => (is  => 'ro', isa => 'Str', required => 1);

=item C<bandwidthAllocated>

Bandwidth allocated to this circuit. Set by the backend after a circuit is established

=cut 
has 'bandwidthAllocated'    => (is  => 'rw', isa => 'PositiveNum', default => 0);

=item C<bandwidthRequested>

Bandwidth requested for this circuit by the ResourceManager. Set at request

=cut 
has 'bandwidthRequested'    => (is  => 'rw', isa => 'PositiveNum', default => 0);

=item C<bandwidthUsed>

Bandwidth actually used by the circuit. No one sets it for now...

=cut 
has 'bandwidthUsed'         => (is  => 'rw', isa => 'PositiveNum', default => 0);

=item C<establishedTime>

UNIX time at which the circuit has been established (reservation succeeded)

=cut
has 'establishedTime'       => (is  => 'rw', isa => 'PositiveNum');

=item C<failures>

Moose array of Failure objects. It is used to keep track of each failures 
which occur on a circuit.

The Moose system provides several helper methods: I<addFailure>, I<getFailure>, 
I<getAllFailures>, I<hasFailures>, I<countFailures> and I<filterFailures>

I<filterFailures> requires a subroutine argument which implements the 
matching logic (elements available to sub in $_);

Each time a value is added, a call to _healthCheck is triggered.

=cut
has 'failures'              => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Common::Failure]', 
                                traits  => ['Array'], 
                                handles => {addFailure      => 'push', 
                                            getFailure      => 'get',
                                            getAllFailures  => 'elements',
                                            hasFailures     => 'is_empty',
                                            countFailures   => 'count',
                                            filterFailures  => 'grep'},
                                trigger =>  \&_healthCheck);

=item C<id>

Randomly generated ID

=cut 
has 'id'                    => (is  => 'ro', isa => 'Str', default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()); });

=item C<lifetime>

Lifetime of the circuit given in seconds

=cut
has 'lifetime'              => (is  => 'rw', isa => 'PositiveNum', default => 6*HOUR);

=item C<maxFailureCount>

Number of failures recorded, after which the circuit will be taken offline

=cut
has 'maxFailureCount'       => (is  => 'rw', isa => 'PositiveNum', default => 1000);

=item C<path>

Path object (holding the nodes), on which this circuit operates on

=cut
has 'path'                  => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::Core::Path', required => 1);

=item C<physicalId>

A string holding the ID of the physical circuit which corresponds to this abstraction.
For example, in the case of NSI, we'd hold the ConnectionID. 

This physical ID will be used to know which reservation to terminate (from the backend point of view)

=cut
has 'physicalId'            => (is  => 'rw', isa => 'Str');

=item C<requestedTime>

UNIX time when the circuit was requested

=cut
has 'requestedTime'         => (is  => 'rw', isa => 'PositiveNum');

=item C<scope>

Not used at the moment, but will be useful when dealing with multiple circuits on the same link.
For example one could use a circuit for "Generic" transfers, and another for "Analysis"

=cut
has 'scope'                 => (is  => 'rw', isa => 'ScopeType',    default => 'Generic');

=item C<status>

Circuit status can be one of the following values: Created Offline Requesting Online Updating.
Not all state jumps are valid (for example we cannot go from Created to Online without going
through Requesting)

=cut
has 'status'                => (is  => 'rw', isa => 'StatusType',   default => 'Created');

=item C<stateDir>

Folder in which the object will be serialized

=cut
has 'stateDir'              => (is  => 'rw', isa => 'Str',          default => '/tmp/resources');

=item C<requestedTime>

UNIX time when this object last changed its status

=back

=cut
has 'lastStatusChange'      => (is  => 'rw', isa => 'PositiveNum',  default => &mytimeofday());
has 'verbose'               => (is  => 'rw', isa => 'Bool',         default => 1);

=head1 METHODS

=over
 
=item C<_healthCheck>

This method is only triggered internally when a failure is recorded.
If the failure count reaches maxFailureCount, the resource will be put in Offline mode.

TODO: Check if it's possible to add a callback attribute. It would be useful to notify 
the ResourceManager when the state changes. Remains to be checked how it handles 
serialisation.

=cut
sub _healthCheck {
    my ($self, $newState, $oldState) = @_;
    my $msg = "NetworkResource->healthCheck";
    if ($self->countFailures > $self->maxFailureCount) {
        $self->Logmsg("$msg: Maximum failure count has been exceeded. Taking the circuit offline");
        $self->setStatus("Offline");
    }
}

=item C<setStatus>

Updates the status of this resource. The state machine logic is implemented here
by a simple switch case (relatively few states, compared to NSI state machine).

It takes it up to two arguments:

Arg0: bwRequested (if requesting or updating) or bwAllocated (if switching to online)

Arg1: physicalID (only used when switching to online)

=cut

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

=item C<getExpirationTime>

Returns the expiration time if the resource is Online

=cut
sub getExpirationTime {
    my $self = shift;

    if ($self->status ne 'Online' || ! defined $self->establishedTime) {
        $self->Logmsg("NetworkResource->getExpirationTime: Resource is not yet online, so to established time has been set");
        return undef;
    }

    return $self->establishedTime + $self->lifetime;
}

=item C<isExpired>

Checks to see if the resource expired or not

=cut
sub isExpired {
    my $self = shift;
    my $expiration = $self->getExpirationTime;
    if (! defined $expiration) {
        $self->Logmsg("NetworkResource->isExpired: Resource is not yet online, so to established time has been set");
        return 0;
    };
    return $expiration <= &mytimeofday(); 
}

=item C<getLinkName>

Simply link name as given by the Path object. The link name is gotten from 
the app name (how PhEDEx/Panda call the nodes)

=cut
sub getLinkName {
    my $self = shift;
    return $self->path->getName;
}

=item C<getSaveLocation>

Returns the location where this resource will be saved

=cut
sub getSaveLocation {
    my $self = shift;
    return $self->stateDir.'/'.$self->status;
}

=item C<getSaveFilename>

Returns the filename of the current resource. It has the following form: 
    NodeA-[to]-NodeB-PartialUID-Date-Time

T2_ANSE_Amsterdam-to-T2_ANSE_Geneva-xyzxyzx-20140427-10:00:00, for a unidirectional link

T2_ANSE_Amsterdam-T2_ANSE_Geneva-xyzxyzx-20140427-10:00:00, for a bidirectional link

=cut
sub getSaveFilename {
    my $self = shift;

    my $partialID = substr($self->id, 1, 7);
    my $formattedTime = &getFormattedTime($self->lastStatusChange);
    my $link = $self->getLinkName();
    my $fileName = $link."-$partialID-".$formattedTime;
    
    return $fileName;
}

=item C<getFullSavePath>

Simply returns the full save path (folder/filename)

=cut
sub getFullSavePath {
    my $self = shift;
    return $self->getSaveLocation()."/".$self->getSaveFilename();
}

=item C<saveState>

Saves the current state of the resource

=cut
sub saveState { 
    my ($self, $overrideLocation) = @_;
    my $msg = 'NetworkResource->saveState';

    # Check if state folder existed and attempt to create if it didn't
    my $location = defined $overrideLocation ? $overrideLocation : $self->getSaveLocation();
    
    my $result = &validateLocation($location);
    if ($result != OK) {
        $self->Logmsg("$msg: Cannot validate location");
        return $result;
    };
    
    # Attempt to save the object
    my $fullPath = $location."/".$self->getSaveFilename();
    $self->store($fullPath);
}

=item C<removeState>

# Attempts to remove the state file associated with this circuit

=cut
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