package PHEDEX::File::Download::Circuits::Backend::Core::Core;

use Moose;

use base 'PHEDEX::Core::Logging', 'Exporter';

use Data::UUID;
use List::Util qw(min);
use POE;

use PHEDEX::File::Download::Circuits::Backend::Core::ResourceRequest;
use PHEDEX::File::Download::Circuits::Common::EnumDefinitions;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;

## Define the constants specific to Backend::Core ##
our @EXPORT = qw(
                REQUEST_SUCCEEDED UPDATE_SUCCEEDED TERMINATE_SUCCEEDED
                REQUEST_FAILED UPDATE_FAILED TERMINATE_FAILED
                );

use constant {
    REQUEST_SUCCEEDED       =>          1,
    UPDATE_SUCCEEDED        =>          2,
    TERMINATE_SUCCEEDED     =>          3,
    REQUEST_FAILED          =>          -1,
    UPDATE_FAILED           =>          -2,
    TERMINATE_FAILED        =>          -3,
};

################## Define the Moose class attributes ##################
has 'id'                    => (is  => 'ro', isa => 'Str', default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()) });

# These are the paths on which this backend can create circuits (# of Path.name => Path)
has 'availablePaths'        => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::Core::Path]',
                                traits  => ['Hash'], 
                                handles => {addPath     => 'set',
                                            getPath     => 'get',
                                            getPaths    => 'values',
                                            pathExists  => 'exists',
                                            deletePath  => 'delete'});

# Hash of all reasources which are currently online (active circuits) (# of LinkID => ResourcesSet)
has 'activeResources'       => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet]',
                                traits  => ['Hash'], 
                                handles => {addActiveSet    => 'set', 
                                            getActiveSet    => 'get',
                                            getActiveSets   => 'values',
                                            activeSetExists => 'exists',
                                            deleteActiveSet => 'delete'});

# Hash of all resources that are pending (pending requests) (# of LinkID => ResourcesSet)
has 'pendingResources'       => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet]',
                                 traits  => ['Hash'], 
                                 handles => {addPendingSet      => 'set', 
                                             getPendingSet      => 'get',
                                             getPendingSets     => 'values',
                                             pendingSetExists   => 'exists',
                                             deletePendingSet   => 'delete'});

has 'maxResources'  => (is  => 'rw', isa => 'Int', default => 10);
has 'verbose'       => (is  => 'rw', isa => 'Bool', default => 0);

# Initialize all POE events specifically related to circuits
sub _poe_init {
    my ($self, $kernel, $session) = @_;
    # Declare events which are going to be used by the ResourceManager
    my @poe_subs = qw(backendRequestResource backendUpdateResource backendTeardownResource);
    $kernel->state($_, $self) foreach @poe_subs;
}

sub moveFromPendingToActive {
    my ($self, $resource) = @_;
    $self->addToActive($self->removeFromPending($resource));
}

sub addToPending {
    my ($self, $resource) = @_;
    return undef if ! defined $resource;
    my $pendingSet = $self->getPendingSet($resource->path->getName);
    if (! defined $pendingSet) {
        $pendingSet = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => $self->maxResources);
        $self->addPendingSet($resource->path->getName(), $pendingSet);
    }
    return $pendingSet->addResource($resource);
}

sub removeFromPending {
    my ($self, $resource) = @_;
    return undef if ! defined $resource;
    my $pendingSet = $self->getPendingSet($resource->path->getName);
    return undef if (! defined $pendingSet);
    return $pendingSet->deleteResource($resource);
}

sub addToActive {
    my ($self, $resource) = @_;
    return undef if ! defined $resource;
    my $activeSet = $self->getActiveSet($resource->path->getName);
    if (! defined $activeSet) {
        $activeSet = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => $self->maxResources);
        $self->addActiveSet($resource->path->getName(), $activeSet);
        
    }
    return $activeSet->addResource($resource);
}

sub removeFromActive {
    my ($self, $resource) = @_;
    return undef if ! defined $resource;
    my $activeSet = $self->getActiveSet($resource->path->getName);
    return undef if (! defined $activeSet);
    return $activeSet->deleteResource($resource);
}

sub getPathBySiteNames {
    my ($self, $siteA, $siteB, $bidirectional) = @_;
    my $pathName = PHEDEX::File::Download::Circuits::Helpers::Utils::Utils::getPath($siteA, $siteB, $bidirectional);
    my $path = $self->getPath($pathName);
    return $path;
}

sub canRequestResource {
    my ($self, $resourceRequest) = @_;

    my $msg = "Core->canRequestResource";
    my $path = $self->getPathBySiteNames($resourceRequest->siteA, $resourceRequest->siteB, $resourceRequest->bidirectional);
    
    # Check if we can even request circuits on this path
    if (! defined $path) {
        $self->Logmsg("$msg: Cannot request a resource between ".$resourceRequest->siteA." and ".$resourceRequest->siteB);
        return undef;
    }
    
    my $pathName = $path->getName();
    
    # If we don't have an active resource set, then there's no active resource on this path
    if (! $self->activeSetExists($pathName)) {
        $self->Logmsg("$msg: Can request resource on path $pathName") if $self->verbose;
        return 1;
    }
    
    # Get resource set for this path
    my $activeSet = $self->getActiveSet($pathName);

    # Check if we haven't exceeded the max number of circuits per path
    if ($activeSet->countResources >= $activeSet->maxResources) {
        $self->Logmsg("$msg: Cannot request a resource on path $pathName at this time (#active = #max)");
        return undef;
    }
    
    # If we still have slots in active and there are no pending requests, then we can request a circuit
    if (! $self->pendingSetExists($pathName)) {
        $self->Logmsg("$msg: Can request resource on path $pathName") if $self->verbose;
        return 1;
    }
    
    # If we get to here, there's always going to be a pending set defined for this path
    my $pendingSet = $self->getPendingSet($pathName);
     # Ensure that by queuing this request we're not exceeding the number of maximum resources
    if (($pendingSet->countResources + $activeSet->countResources) >= $activeSet->maxResources) {
        $self->Logmsg("$msg: Cannot request a resource on path $pathName at this time. (#pending + #active = #max)");
        return undef;
    }
    
    return 1;
}

sub hasResource {
    my ($self, $resource) = @_;
    my $msg = "Core->hasResource"; 
    
    if (! $self->activeSetExists($resource->path->getName)) {
        $self->Logmsg("$msg: There is not resource set for this path");
        return undef;
    }
    
    my $activeSet = $self->getActiveSet($resource->path->getName);
    
    if (! $activeSet->resourceExists($resource)) {
        $self->Logmsg("$msg: Resource doesn't exist in the active set");
        return undef;
    }
    
    return 1;
}

# Does the initial leg work for a resource request
sub backendRequestResource {
    my ($self, $kernel, $session, $resourceRequest) = @_[ OBJECT, KERNEL, SESSION, ARG0];
    my $msg = "Core->backendRequestResource";

    if (! defined $resourceRequest)  {
        $self->Logmsg("$msg: Some arguments are not defined");
        return;
    }

    # Recheck if we can actually request a resource
    my $canRequest = $self->canRequestResource($resourceRequest);
    
    if (! $canRequest) {
        $self->Logmsg("$msg: Cannot request resource");
        $resourceRequest->callback->(undef, REQUEST_FAILED);
    }
}

# Does the initial leg work for a resource update
sub backendUpdateResource {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    my $msg = "Core->backendUpdateResource";
    
    if (! defined $resource || ! defined $callback) {
        $self->Logmsg("$msg: Some arguments are not defined");
        return;
    }
    
    if (! $self->hasResource($resource)) {
        $self->Logmsg("$msg: Cannot update a resource which doesn't exist");
        $callback->(undef, UPDATE_FAILED);
    }
}

# Does the initial leg work for a resource teardown
sub backendTeardownResource {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    my $msg = "Core->backendTeardownResource";
    
    if (! defined $resource || ! defined $callback) {
        $self->Logmsg("$msg: Some arguments are not defined");
        return;
    }
    
    if (! $self->hasResource($resource)) {
        $self->Logmsg("$msg: Cannot terminate a resource which doesn't exist");
        $callback->(undef, TERMINATE_FAILED);
    }
}

1;