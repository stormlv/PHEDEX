package PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;

use Moose;

has 'resources'             => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource]',      # {NetworkResource.ID => NetworkResource}
                                traits  => ['Hash'], 
                                handles => {countResources  => 'count',
                                            isEmpty         => 'is_empty',
                                            getAllResources => 'values'});

has 'maxResources' => (is  => 'rw', isa => 'Int', required => 1);

sub resourceExists {
    my ($self, $resource) = @_;
    return defined $resource && defined $self->resources->{$resource->id};
}

sub getResource {
    my ($self, $resource) = @_;
    return undef if ! $self->resourceExists($resource);
    return $self->resources->{$resource->id};
}

sub deleteResource {
    my ($self, $resource) = @_;
    return undef if ! $self->resourceExists($resource);
    delete $self->resources->{$resource->id};
    return $resource;
}

# Checks if a resource can be added or not
sub canAddResource {
    my $self = shift;
    return $self->countResources < $self->maxResources;
}

# Attempts to add the resource to the set.
# Returns the resources if it has been added, or undef if it failed
sub addResource {
    my ($self, $resource) = @_;
    if (! defined $resource || 
        ! $self->canAddResource ||
        $self->resourceExists($resource)) {
        return undef;
    }
    $self->resources->{$resource->id} = $resource;
    return $resource;
}

# Returns the first available resource that matches the scope
sub getResourceByScope {
    my ($self, $scope) = @_;
    return undef if $self->isEmpty;
    my @resources = $self->getAllResources;
    foreach my $resource (@resources) {
        return $resource if $resource->scope eq $scope;
    }
    return undef;
}

# Returns the resource that has the highest bandwidth allocated
sub getResourceByBW {
    my $self = shift;
    return undef if $self->isEmpty;
    my @resources = $self->getAllResources;
    my $maxBW = 0;
    my $maxResource = undef;
    foreach my $resource (@resources) {
        if ($resource->bandwidthAllocated > $maxBW) {
            $maxBW = $resource->bandwidthAllocated;
            $maxResource = $resource;
        }
    }
    return $maxResource;
}

1;