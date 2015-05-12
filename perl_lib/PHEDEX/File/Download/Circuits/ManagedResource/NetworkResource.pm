package PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

use Moose;
use base 'PHEDEX::Core::Logging', 'Exporter';

use Data::Dumper;
use Data::UUID;
use File::Path;
use POSIX;
use Time::HiRes qw(time);

use PHEDEX::Core::Command;
use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Common::EnumDefinitions;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;

$Data::Dumper::Terse = 1;
$Data::Dumper::Indent = 1;

our @EXPORT = qw(openState);


# Define enums
use Moose::Util::TypeConstraints;
    enum 'ResourceType',    RESOURCE_TYPES;
    enum 'ScopeType',       SCOPE_TYPES;
    enum 'StatusType',      STATUS_TYPES;
no Moose::Util::TypeConstraints;

# TODO Add the ID of the physical circuit to which this network resource is mapped to
 
# Required attributes
has 'backendType'       => (is  => 'ro', isa => 'Str' ,         required => 1);
has 'resourceType'      => (is  => 'ro', isa => 'ResourceType', required => 1);
has 'path'              => (is  => 'ro', isa => 'PHEDEX::File::Download::Circuits::ManagedResource::Core::Path', required => 1);

# Pre-initialised attributes
has 'bandwidthAllocated'    => (is  => 'rw', isa => 'Int',          default => 0);
has 'bandwidthRequested'    => (is  => 'rw', isa => 'Int',          default => 0);
has 'bandwidthUsed'         => (is  => 'rw', isa => 'Int',          default => 0);
has 'id'                    => (is  => 'ro', isa => 'Str',          builder => '_createID');
has 'failures'              => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Common::Failure]', traits  => ['Array'], handles => {addFailure => 'push', getFailure => 'get', getAllFailures => 'elements'});
has 'name'                  => (is  => 'rw', isa => 'Str');
has 'scope'                 => (is  => 'rw', isa => 'ScopeType',    default => 'Generic');
has 'status'                => (is  => 'rw', isa => 'StatusType',   default => 'Offline', trigger => sub {my $self = shift; $self->lastStatusChange(&mytimeofday());});
has 'stateDir'              => (is  => 'rw', isa => 'Str',          default => '/tmp/managed');
has 'lastStatusChange'      => (is  => 'rw', isa => 'Num',          default => &mytimeofday());
has 'verbose'               => (is  => 'rw', isa => 'Bool',         default => 1);

# Builder used for the creation of the ID
sub _createID {
    my $ug = new Data::UUID;
    my $id = $ug->to_string($ug->create());
    return $id;
}

sub getLinkName {
    my $self = shift;
    return $self->path->getName;
}

sub getSaveParams {
    my $self = shift;
    $self->Fatal("NetworkResource->getSaveParams: method not implemented in extending class ", __PACKAGE__);
}

# Generates a file name in the form of : NODE_A-(to)-NODE_B-UID-time
# ex. T2_ANSE_Amsterdam-to-T2_ANSE_Geneva-FSDAXSDA-20140427-10:00:00, if link is unidirectional
# or T2_ANSE_Amsterdam-T2_ANSE_Geneva-FSDAXSDA-20140427-10:00:00, if link is bi-directional.
# Returns a save path ({STATE_DIR}/[circuits/bod]/$state) and a file path ({STATE_DIR}/[circuits/bod]/$state/$NODE_A-to-$NODE_B-$time)
sub getSavePaths{
    my $self = shift;
    
    my $msg = "NetworkResource->getSavePaths";
        
    if (! defined $self->id) {
        $self->Logmsg("$msg: Cannot generate a save name for a resource which is not initialised");
        return;
    }
    
    my ($savePath, $saveTime) = $self->getSaveParams();

    if (!defined $savePath || !defined $saveTime || $saveTime <= 0) {
        $self->Logmsg("$msg: Invalid parameters in generating a circuit file name");
        return;
    }

    my $partialID = substr($self->id, 1, 7);
    my $formattedTime = &getFormattedTime($saveTime);
    my $link = $self->getLinkName();
    my $fileName = $link."-$partialID-".$formattedTime;
    my $filePath = $savePath.'/'.$fileName;

    # savePath: {STATE_DIR}/[circuits|bod]/$state
    # fileName: NODE_A-(to)-NODE_B-UID-time
    # filePath (savePath/fileName): {STATE_DIR}/[circuits|bod]/$state/NODE_A-(to)-NODE_B-UID-time
    return ($savePath, $fileName, $filePath);
}

sub checkCorrectPlacement {
    my ($self, $path) = @_;
    
    my $msg = 'NetworkResource->checkCorrectPlacement';
    
    my ($savePath, $fileName, $filePath) = $self->getSavePaths();
    
    if (! defined $savePath || ! defined $filePath) {
        $self->Logmsg("$msg: Cannot generate save name...");
        return ERROR_GENERIC;
    }
    
    if ($filePath ne $path) {
        $self->Logmsg("$msg: Provided filepath doesn't match where object should be located");
        return ERROR_GENERIC;
    } else {
        return OK;
    }
}

# Saves the current state of the resource
# For this a valid STATE_DIR must be defined
# If it's not specified at construction it will automatically be created in /tmp/managed/{RESOURCE_TYPE}
# Depending on the resource type that's being saved, the STATE_DIR
# Based on its current state, the resource will create additional subfolders
# For ex. a circuit will create /requested, /online, /offline
# A managed bandwidth will create /online, offline
sub saveState { 
    my $self = shift;

    my $msg = 'NetworkResource->saveState';

    # Generate file name based on
    my ($savePath, $fileName, $filePath) = $self->getSavePaths();
    if (! defined $filePath) {
        $self->Logmsg("$msg: An error has occured while generating file name");
        return ERROR_GENERIC;
    }

    # Check if state folder existed and create if it didn't
    if (!-d $savePath) {
        File::Path::make_path($savePath, {error => \my $err});
        if (@$err) {
            $self->Logmsg("$msg: State folder did not exist and we were unable to create it");
            return ERROR_PATH_INVALID;
        }
    }

    # Save the resource object
    my $file = &output($filePath, Dumper($self));
    if (! $file) {
        $self->Logmsg("$msg: Unable to save state information");
        return ERROR_SAVING;
    } else {
        $self->Logmsg("$msg: State information successfully saved");
        return OK;
    };
}

# Factory like method : returns a new resource object from a state file on disk
# It will throw an error if the resource provided is corrupt
# i.e. it doesn't have an ID, STATUS, RESOURCE_TYPE, BOOKING_BACKEND or nodes defined
sub openState {
    my $path = shift;

    return ERROR_PARAMETER_UNDEF if ! defined $path;
    return ERROR_FILE_NOT_FOUND if (! -e $path);

    my $resource = &evalinfo($path);

    if (! defined $resource || 
        ! defined $resource->id ||
        ! defined $resource->resourceType ||
        ! defined $resource->status ||
        ! defined $resource->path->nodeA || ! defined $resource->path->nodeB ||
        ! defined $resource->backendType ||
        ! defined $resource->stateDir ||
        ! defined $resource->lastStatusChange) {
        return ERROR_INVALID_OBJECT;
    }

    return $resource;
}

# Attempts to remove the state file associated with this circuit
sub removeState {
    my $self = shift;

    my $msg = 'NetworkResource->removeState';

    my ($savePath, $fileName, $filePath) = $self->getSavePaths();
    if (!-d $savePath || !-e $filePath) {
        $self->Logmsg("$msg: There's nothing to remove from the state folders");
        return ERROR_GENERIC;
    }

    return !(unlink $filePath) ? ERROR_GENERIC : OK;
}

sub TO_JSON {
    return { %{ shift() } };
}

1;