=head1 NAME

ManagedResource::ResourceSet - Set holding resource objects (duh)

=head1 DESCRIPTION

This object is basically a smarter hash of NetworkResource objects.

A limit can be specified on how many objects it holds.

=cut

package PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;

use Moose;
use MooseX::Storage;

with Storage('format' => 'YAML', 'io' => 'File');

use Data::UUID;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;

=head1 ATTRIBUTES

=over
 
=item C<id>

Randomly generated ID

=cut 
has 'id'            => (is  => 'ro', isa => 'Str', default => sub { my $ug = new Data::UUID; $ug->to_string($ug->create()); });

=item C<resources>

It's a Moose hash of ResourceSet objects, taking the NetworkResource.id attribute as key.

The Moose system provides several helper methods: I<getResource>, I<countResources>, I<isEmpty>, I<getAllResources>

=cut
has 'resources'     => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource]',      # {NetworkResource.ID => NetworkResource}
                        traits  => ['Hash'], 
                        handles => {getResource     => 'get',
                                    countResources  => 'count',
                                    isEmpty         => 'is_empty',
                                    getAllResources => 'values'});
=item C<maxResources>

Maximum number of resources which can be added to the has

=back

=cut
has 'maxResources'  => (is  => 'rw', isa => 'Int', required => 1);
has 'stateDir'      => (is  => 'rw', isa => 'Str', default => '/tmp/resources');

=head1 METHODS

=over
 
=item C<resourceExists>

Checks if a give resource exists in the hash

=cut
sub resourceExists {
    my ($self, $resource) = @_;
    return defined $resource && defined $self->resources->{$resource->id};
}

=item C<deleteResource>

Deletes the given resource from the set

=cut
sub deleteResource {
    my ($self, $resource) = @_;
    return undef if ! $self->resourceExists($resource);
    delete $self->resources->{$resource->id};
    return $resource;
}

=item C<canAddResource>

Checks if we can still add a resource (count < max)

=cut
sub canAddResource {
    my $self = shift;
    return $self->countResources < $self->maxResources;
}

=item C<addResource>

Attempts to add the resource to the set. Returns the resources if it has been added, or undef if it failed

=cut
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

=item C<getResourceByScope>

Returns the first available resource that matches the scope

=cut
sub getResourceByScope {
    my ($self, $scope) = @_;
    return undef if $self->isEmpty;
    my @resources = $self->getAllResources;
    foreach my $resource (@resources) {
        return $resource if $resource->scope eq $scope;
    }
    return undef;
}

=item C<getResourceByBW>

Returns the resource that has the highest bandwidth allocated

=cut
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

=item C<saveState>

Saves the current set of resources

=cut
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