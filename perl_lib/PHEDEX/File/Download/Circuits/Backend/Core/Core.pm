package PHEDEX::File::Download::Circuits::Backend::Core::Core;

use Moose;

use base 'PHEDEX::Core::Logging';

use POE;
use List::Util qw(min);

use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;

has 'availablePaths'    => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::Backend::Core::Path]',
                            traits  => ['Hash'], 
                            handles => {getPathByName   => 'get',
                                        getPaths        => 'values',
                                        pathExists      => 'exists',
                                        deletePath      => 'delete'});

sub addPath {
    my ($self, $path) = @_;
    return undef if ! defined $path;
    $self->paths->{$path->getName} = $path;
    return $path;
}

# Initialize all POE events specifically related to circuits
sub _poe_init {
    my ($self, $kernel, $session) = @_;
    # Declare events which are going to be used by the ResourceManager
    my @poe_subs = qw(backendRequestResource backendUpdateResource backendTeardownResource);
  $kernel->state($_, $self) foreach @poe_subs;
}

sub getPathByNodes {
    my ($self, $nodeA, $nodeB, $bidirectional) = @_;
    my $pathName = &getPath($nodeA, $nodeB, $bidirectional);
    return $self->getPath($pathName);
}

# This method should be implemented by the backend child
# It will be called to request the creation of a resource
sub backendRequestResource {
    my $self = shift;
    $self->Fatal("request not implemented by circuit backend ", __PACKAGE__);
}

# This method should be implemented by the backend child
# It will be called update the resource
sub backendUpdateResource {
    my $self = shift;
    $self->Fatal("request not implemented by backend ", __PACKAGE__);
}

# This method should be implemented by the backend child
# It will be called to request the teardown of a resource
sub backendTeardownResource {
    my $self = shift;
    $self->Fatal("teardown not implemented by circuit backend ", __PACKAGE__);
}

1;