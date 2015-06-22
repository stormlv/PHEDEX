=head1 NAME

Backend::Core::Core - Base class for all circuit backends

=head1 DESCRIPTION

This class should not be instantiated on its own... It's only the base class for the circuit backends.

=cut

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

=head1 ATTRIBUTES

=over
 
=item C<id>

Randomly generated ID

=cut 
has 'id'                    => (is  => 'ro', isa => 'Str', default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()) });

=item C<availablePaths>

It's a Moose hash of Path objects, taking the Path.name attribute as key.
It holds the paths on which the backend can request the creation of circuits.

The Moose system provides several helper methods: I<addPath>, I<getPath>, I<getPaths>, I<pathExists>, <deletePath>

=cut
has 'availablePaths'        => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::Core::Path]',
                                traits  => ['Hash'], 
                                handles => {addPath     => 'set',
                                            getPath     => 'get',
                                            getPaths    => 'values',
                                            pathExists  => 'exists',
                                            deletePath  => 'delete'});

=item C<activeResources>

It's a Moose hash of ResourceSet objects, taking the Path.Name attribute as key. It holds all the resources 
which are currently active. These resources are grouped into sets, since for a single path we allow more 
than one circuit to be created.

The Moose system provides several helper methods: I<addActiveSet>, I<getActiveSet>, 
I<getActiveSets>, I<activeSetExists>, I<deleteActiveSet>

=cut
has 'activeResources'       => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet]',
                                traits  => ['Hash'], 
                                handles => {addActiveSet    => 'set', 
                                            getActiveSet    => 'get',
                                            getActiveSets   => 'values',
                                            activeSetExists => 'exists',
                                            deleteActiveSet => 'delete'});

=item C<pendingResources>

It's a Moose hash of ResourceSet objects, taking the Path.Name attribute as key. It holds all the resources 
which are currently pending. These resources are grouped into sets, since for a single path we allow more 
than one circuit to be created.

The Moose system provides several helper methods: I<addPendingSet>, I<getPendingSet>, 
I<getPendingSets>, I<pendingSetExists>, I<deletePendingSet>

=cut
has 'pendingResources'       => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet]',
                                 traits  => ['Hash'], 
                                 handles => {addPendingSet      => 'set', 
                                             getPendingSet      => 'get',
                                             getPendingSets     => 'values',
                                             pendingSetExists   => 'exists',
                                             deletePendingSet   => 'delete'});
                                             
=item C<stateDir>

Folder in which the object will be serialized

=cut
has 'stateDir'      => (is  => 'rw', isa => 'Str', default => 10);

=item C<maxResources>

Maximum number of concurrent circuit per each path

=back

=cut
has 'maxResources'  => (is  => 'rw', isa => 'Int', default => 10);
has 'verbose'       => (is  => 'rw', isa => 'Bool', default => 0);

=head1 METHODS

=over
 
=item C<_poe_init>

Initialize all POE events specifically related to circuits

=cut

sub _poe_init {
    my ($self, $kernel, $session) = @_;
    # Declare events which are going to be used by the ResourceManager
    my @poe_subs = qw(backendRequestResource backendUpdateResource backendTeardownResource);
    $kernel->state($_, $self) foreach @poe_subs;
}

=item C<moveFromPendingToActive>

Moves a resource from the pending set to the active set

=cut

sub moveFromPendingToActive {
    my ($self, $resource) = @_;
    $self->addToActive($self->removeFromPending($resource));
}

=item C<addToPending>

Adds a resource to the pending set. Creates a new set if none existed for a given path. 
Returns the added resource if there were no errors

=cut

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

=item C<removeFromPending>

Removes a resource from the pending set. Returns the removed resource if there were no errors

=cut
sub removeFromPending {
    my ($self, $resource) = @_;
    return undef if ! defined $resource;
    my $pendingSet = $self->getPendingSet($resource->path->getName);
    return undef if (! defined $pendingSet);
    return $pendingSet->deleteResource($resource);
}

=item C<addToActive>

Adds a resource to the active set. Creates a new set if none existed for a given path. 
Returns the added resource if there were no errors

=cut
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

=item C<removeFromActive>

Removes a resource from the active set. Returns the removed resource if there were no errors

=cut
sub removeFromActive {
    my ($self, $resource) = @_;
    return undef if ! defined $resource;
    my $activeSet = $self->getActiveSet($resource->path->getName);
    return undef if (! defined $activeSet);
    return $activeSet->deleteResource($resource);
}

=item C<getPathBySiteNames>

Takes in two site names (SiteA, SiteB)and a boolean (Bidirectional), and attempts to retrieve a 
Path object from the "availablePaths" attribute

=cut
sub getPathBySiteNames {
    my ($self, $siteA, $siteB, $bidirectional) = @_;
    my $pathName = PHEDEX::File::Download::Circuits::Helpers::Utils::Utils::getPath($siteA, $siteB, $bidirectional);
    my $path = $self->getPath($pathName);
    return $path;
}

=item C<canRequestResource>

Returns 1 if a new circuit request can be made of the specified path, or undef otherwise. Takes in a ResourceRequest object.
Checks if
- the path requested allows for circuits
- if we haven't exceeded the number of maximum circuits on that path (either active or pending)

=cut
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

=item C<hasResource>

Returns 1 if the provided resource is in an active set.

=cut
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

=item C<backendRequestResource>

Does the initial leg work for a resource request. Will be called by the extending class 

=cut
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

=item C<backendUpdateResource>

Does the initial leg work for a resource update. Will be called by the extending class 

=cut
sub backendUpdateResource {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    my $msg = "Core->backendUpdateResource";

    $self->Logmsg("$msg: This method is not supported ATM");

    if (! defined $resource || ! defined $callback) {
        $self->Logmsg("$msg: Some arguments are not defined");
        return;
    }
    
    if (! $self->hasResource($resource)) {
        $self->Logmsg("$msg: Cannot update a resource which doesn't exist");
        $callback->(undef, UPDATE_FAILED);
    }
}

=item C<backendUpdateResource>

Does the initial leg work for a resource teardown. Will be called by the extending class 

=back

=cut
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