=head1 NAME

Backend::Other::Dummy - Dummy backend, only used for testing

=head1 DESCRIPTION

This backend extends the Core::Core class and provides dummy output 
for all the requests (backendRequestResource, backendUpdateResource, backendTeardownResource) 
coming from the ResourceManager (or any other entity).

A timeSimulation attribute can be set which simulates the time delay (in seconds)
 to get a reply from the backend.

By default only a combination of the following nodes is a valid path on which you can request
circuits: T2_ANSE_CERN_Dev, T2_ANSE_CERN_0, T2_ANSE_CERN_1, T2_ANSE_CERN_2
=cut

package PHEDEX::File::Download::Circuits::Backend::Other::Dummy;

use Moose;
extends 'PHEDEX::File::Download::Circuits::Backend::Core::Core';

use POE;
use List::Util qw[min max];

use PHEDEX::Core::Command;
use PHEDEX::File::Download::Circuits::Backend::Core::Core;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

has 'timeSimulation'    => (is  => 'rw', isa => 'Num', default => 5); # 

sub BUILD {
    my $self = shift;
    my @nodes;

    # Add dummy nodes
    $nodes[0] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_Dev', netName => '137.138.42.16');
    $nodes[1] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_0', netName => '188.184.134.192');
    $nodes[2] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_1', netName => '128.142.135.112');
    $nodes[3] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_2', netName => '127.0.0.1');

    # Add dummy paths
    for (my $i = 0; $i < 4; $i++) {
        for (my $j = $i + 1; $j < 4; $j++) {
            my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodes[$i], nodeB => $nodes[$j], type => 'Layer2');
            $self->addPath($path->getName, $path);
        }
    }
}

override '_poe_init' => sub {
    my ($self, $kernel, $session) = @_;
    super();
    # Create the action which is going to be called on STDOUT by External
    $kernel->state('delayedAction', $self);
};

=head1 METHODS

=over
 
=item C<backendRequestResource>

Extended method from Core::Core. If the timesimulation is >= 0, the request will be delayed, but will succeed.
If it is negative, the request will fail.

=cut
override 'backendRequestResource' => sub {
    my ($self, $kernel, $session, $request) = @_[ OBJECT, KERNEL, SESSION, ARG0];
    super();
    my $msg = "Dummy->backendRequestResource";

    my $path = $self->getPathBySiteNames($request->siteA, $request->siteB, $request->bidirectional);
    my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendName => 'NSI', resourceType  => 'Circuit', path => $path);
    
    if ($self->timeSimulation >= 0) {
        $self->Logmsg("$msg: Dummy resource created for path ".$path->getName." with a BW of ".$resource->bandwidthRequested());
        $self->addToActive($resource);
        $kernel->delay_add('delayedAction' => $self->timeSimulation, $resource, $request->callback, REQUEST_SUCCEEDED);
    } else {
        $self->Logmsg("$msg: Dummy resource creation failed for path ".$path);
        $kernel->delay_add('delayedAction' => -$self->timeSimulation, $resource, $request->callback, REQUEST_FAILED);
    }
};

=item C<backendUpdateResource>

Extended method from Core::Core. Same as for backendRequestResource,
if the timesimulation is >= 0, the request will be delayed, but will succeed.
If it is negative, the request will fail.

=cut
override 'backendUpdateResource' => sub {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    super();
    my $msg = "Dummy->backendUpdateResource";
    $self->Logmsg("$msg: Updating resource");
    if ($self->timeSimulation >= 0) {
        $kernel->delay_add('delayedAction' => $self->timeSimulation, $resource, $callback, UPDATE_SUCCEEDED);
    } else {
        $kernel->delay_add('delayedAction' => -$self->timeSimulation, $resource, $callback, UPDATE_FAILED);
    }
};

=item C<backendTeardownResource>

Extended method from Core::Core. Removes the resource from active and calls back with TERMINATE_SUCCEEDED

=back

=cut
override 'backendTeardownResource' => sub {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    super();
    my $msg = "Dummy->backendTeardownResource";
    $self->Logmsg("$msg: Tearing down resource");
    $self->removeFromActive($resource);
    $callback->($resource, TERMINATE_SUCCEEDED);
};

sub delayedAction {
     my ($resource, $callback, $status) = @_[ARG0, ARG1, ARG2];
     $callback->($resource, $status);
}

1;

