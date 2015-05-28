package PHEDEX::Tests::File::Download::CircuitBackends::Core::TestCore;

use strict;
use warnings;

use PHEDEX::File::Download::Circuits::Backend::Core::Core;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

use POE;
use Test::More;

sub testGetPathBySiteNames {
    my $core = PHEDEX::File::Download::Circuits::Backend::Core::Core->new();
    my $msg = "TestCore->getPathBySiteNames";
    # Add nodes
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeA', netName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeB', netName => 'STP2', maxBandwidth => 222);
    # Add path
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    $core->addPath($path->getName(), $path);

    my $testPath1 = $core->getPathBySiteNames('nodeA', 'nodeB', 1);
    my $testPath2 = $core->getPathBySiteNames('nodeB', 'nodeA', 1);
    my $testPath3 = $core->getPathBySiteNames('nodeA', 'nodeB', 0);
    my $testPath4 = $core->getPathBySiteNames('nodeB', 'nodeA', 0);

    is_deeply($testPath1, $path, "$msg: Correctly identified path nodeA-NodeB via site names");
    is_deeply($testPath2, $path, "$msg: Correctly identified path nodeB-NodeA via site names");
    ok(! $testPath3, "$msg: Path nodeA-to-NodeB doesn't exist");
    ok(! $testPath4, "$msg: Path nodeB-to-NodeA doesn't exist");
}

sub testCanRequestResource {
    my $core = PHEDEX::File::Download::Circuits::Backend::Core::Core->new();
    my $msg = "TestCore->testCanRequestResource";
    # Add nodes
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeA', netName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeB', netName => 'STP2', maxBandwidth => 222);
    # Add path
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    $core->addPath($path->getName(), $path);
    
    ok($core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can request resource on path nodeA-nodeB");
    ok($core->canRequestResource('nodeB', 'nodeA', 1), "$msg: Can request resource on path nodeB-nodeA");
    ok(! $core->canRequestResource('nodeA', 'nodeB', 0), "$msg: Can request resource on path nodeA-to-nodeB");
    ok(! $core->canRequestResource('nodeB', 'nodeA', 0), "$msg: Can request resource on path nodeB-to-nodeA");
    
    # Add active sets
    my $activeSet = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => $core->maxResources);
    $core->addActiveSet($path->getName(), $activeSet);
    for (my $i = 0; $i < 5; $i++ ) {
        my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
        $activeSet->addResource($resource);
    }
    ok($core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can request resource on path nodeA-nodeB");
    
    my $pendingSet = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => $core->maxResources);
    $core->addPendingSet($path->getName(), $pendingSet);
    for (my $i = 0; $i < 4; $i++ ) {
        my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
        $pendingSet->addResource($resource);
    }
    ok($core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can request resource on path nodeA-nodeB");
    
    my $lastPendingResource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType  => 'Circuit', path => $path);
    $pendingSet->addResource($lastPendingResource);
    
    ok(! $core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can no longer request resource on path nodeA-nodeB");
    
    $pendingSet->deleteResource($lastPendingResource);
    ok($core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can request resource on path nodeA-nodeB");
    
    my $activeResource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
    $activeSet->addResource($activeResource);
    ok(! $core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can no longer request resource on path nodeA-nodeB");
    
    for (my $i = 0; $i < 5; $i++ ) {
        my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
        $activeSet->addResource($resource);
    }
    ok(! $core->canRequestResource('nodeA', 'nodeB', 1), "$msg: Can no longer request resource on path nodeA-nodeB");
}

sub testHasResource {
    my $core = PHEDEX::File::Download::Circuits::Backend::Core::Core->new();
    my $msg = "TestCore->testHasResource";
    # Add nodes
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeA', netName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeB', netName => 'STP2', maxBandwidth => 222);
    # Add path
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    $core->addPath($path->getName(), $path);
    # Add active set
    my $activeSet = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => $core->maxResources);
    $core->addActiveSet($path->getName(), $activeSet);
    my $resource1 = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
    my $resource2 = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
    # Add resource to active set
    $activeSet->addResource($resource1);

    ok($core->hasResource($resource1), '$msg: Correctly found resource 1');
    ok(!$core->hasResource($resource2), '$msg: Resource 2 does not exist');
}

sub setupSession {
    my $core = shift;
    
    my $states;

    $states->{_start} = sub {
        my ($kernel, $session) = @_[KERNEL, SESSION];
        $core->Logmsg("Starting a POE test session (id=",$session->ID,")");
        $core->_poe_init($kernel, $session);
        
        # Create callbacks
        my $requestCallback = $session->postback("requestCallback");
        my $updateCallback = $session->postback("updateCallback");
        my $terminationCallback = $session->postback("terminationCallback");
        
        my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeA', netName => 'STP1', maxBandwidth => 111);
        my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'nodeB', netName => 'STP2', maxBandwidth => 222);
        my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
        my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy', resourceType    => 'Circuit', path => $path);
    
        # Delayed request of a circuit
        $kernel->delay('backendRequestResource' => 0.5, "NodeA", "NodeB", 1, $requestCallback);
        $kernel->delay('backendUpdateResource' => 1, $resource, $updateCallback);
        $kernel->delay('backendTeardownResource' => 1.5, $resource, $terminationCallback);
    };
    
    $states->{requestCallback} = \&requestCallback;
    $states->{updateCallback} = \&updateCallback;
    $states->{terminationCallback} = \&terminationCallback;

    my $session = POE::Session->create(inline_states => $states);

    return $session;
}

sub requestCallback {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $replyCode = $arguments->[1];
    is($replyCode, REQUEST_FAILED, "TestCore->requestCallback: Request failed");
}

sub updateCallback {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $replyCode = $arguments->[1];
    is($replyCode, UPDATE_FAILED, "TestCore->updateCallback: Update failed");
}

sub terminationCallback {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $replyCode = $arguments->[1];
    is($replyCode, TERMINATE_FAILED, "TestCore->terminationCallback: Termination failed");
}

sub testPOEStuff {
    my $core = PHEDEX::File::Download::Circuits::Backend::Core::Core->new();
    my $session = setupSession($core);
    POE::Kernel->run();
}

testGetPathBySiteNames();
testCanRequestResource();
testHasResource();
testPOEStuff();


done_testing;

1;
