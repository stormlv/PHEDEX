package PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;

use Moose;
use MooseX::Storage;

with Storage('format' => 'YAML', 'io' => 'File');

use Data::UUID;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;


has 'id'            => (is  => 'ro', isa => 'Str', 
                        default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()); });
has 'resources'     => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource]',      # {NetworkResource.ID => NetworkResource}
                        traits  => ['Hash'], 
                        handles => {countResources  => 'count',
                                    isEmpty         => 'is_empty',
                                    getAllResources => 'values'});
has 'maxResources'  => (is  => 'rw', isa => 'Int', required => 1);
has 'stateDir'      => (is  => 'rw', isa => 'Str', default => '/tmp/resources');

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

# Saves the set
sub saveState { 
    my ($self, $overrideLocation) = @_;
    my $msg = 'ResourceSet->saveState';

    # Check if state folder existed and attempt to create if it didn't
    my $location = defined $overrideLocation ? $overrideLocation : $self->stateDir;
 
    my $result = &validateLocation($location);
    if ($result != OK) {
        $self->Logmsg("$msg: Cannot validate location");
        return $result;
    };
    
    $self->store($location."/".$self->id.".set");
}

1;